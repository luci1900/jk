package operator

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/luci1900/jk/api/v1alpha1"
)

// Task states (v1alpha1.ActionState values).
const (
	StatePending   = v1alpha1.ActionState("pending")
	StateRunning   = v1alpha1.ActionState("running")
	StateCompleted = v1alpha1.ActionState("completed")
	StateFailed    = v1alpha1.ActionState("failed")
	StateCancelled = v1alpha1.ActionState("cancelled")
	StateAborting  = v1alpha1.ActionState("aborting")
	StateAborted   = v1alpha1.ActionState("aborted")
)

// ActionTerminal says whether a state is final.
func ActionTerminal(s v1alpha1.ActionState) bool {
	switch s {
	case StateCompleted, StateFailed, StateCancelled, StateAborted:
		return true
	}
	return false
}

// MaxActionResultsAgeKey is the jk-model key of how long finished Actions are kept (juju's max-action-results-age).
const MaxActionResultsAgeKey = "model-config.max-action-results-age"

// DefaultActionResultsAge is juju's default for max-action-results-age.
const DefaultActionResultsAge = 336 * time.Hour

// UnitLife is how an action's target unit stands.
type UnitLife int

const (
	// UnitLive: the unit exists (or its ordinal is within the scale and its pod is yet to come).
	UnitLive UnitLife = iota
	// UnitDying: the unit is still there but on its way out (scaled away, or its application is being removed).
	UnitDying
	// UnitGone: no such unit.
	UnitGone
)

// ParseUnit splits "app/3".
func ParseUnit(unit string) (app string, n int, ok bool) {
	app, num, found := strings.Cut(unit, "/")
	if !found || app == "" || num == "" || (len(num) > 1 && num[0] == '0') {
		return "", 0, false
	}
	n, err := strconv.Atoi(num)
	if err != nil || n < 0 || strconv.Itoa(n) != num {
		return "", 0, false
	}
	return app, n, true
}

// ClassifyUnit decides how the unit with ordinal n stands, given its application's scale and whether the unit still
// has a pod or UnitData (it can outlive the scale while it runs its stop hooks).
func ClassifyUnit(n int, scale int32, appDeleting, hasPod, hasUnitData bool) UnitLife {
	within := int32(n) < scale
	switch {
	case !within && !hasPod && !hasUnitData:
		return UnitGone
	case appDeleting && !hasPod && !hasUnitData:
		return UnitGone
	case !within || appDeleting:
		return UnitDying
	}
	return UnitLive
}

// ActionAdmission is the verdict on a new Action.
type ActionAdmission struct {
	// State is StatePending to run it, or StateFailed with the reason in Message.
	State   v1alpha1.ActionState
	Message string
	// Parameters are the validated parameters with defaults filled in (pending only).
	Parameters map[string]any
}

// AdmitAction validates a new Action: the unit must exist and not be dying, the action must be defined by the charm
// (schemas from ActionSchemas) and the parameters must satisfy its schema. Defaults are filled in after validation, as
// juju does.
func AdmitAction(life UnitLife, unit, name string, params map[string]any, schemas map[string]map[string]any) ActionAdmission {
	fail := func(format string, args ...any) ActionAdmission {
		return ActionAdmission{State: StateFailed, Message: fmt.Sprintf(format, args...)}
	}
	switch life {
	case UnitGone:
		return fail("unit %q not found", unit)
	case UnitDying:
		return fail("unit %q is dying", unit)
	}
	schema, ok := schemas[name]
	if !ok {
		names := make([]string, 0, len(schemas))
		for n := range schemas {
			names = append(names, n)
		}
		sort.Strings(names)
		return fail("action %q is not defined by the charm (actions: %s)", name, strings.Join(names, ", "))
	}
	if errs := ValidateActionParams(schema, params); len(errs) > 0 {
		return fail("validation failed: %s", strings.Join(errs, "; "))
	}
	return ActionAdmission{State: StatePending, Parameters: InsertActionDefaults(schema, params)}
}

// SettleAction says how an already admitted, unfinished Action ends when its unit is dying or gone; ok is false when
// it should be left to the unit's agent. Pending actions never ran: they fail on a dying unit and are cancelled on a
// removed one; a running one of a removed unit is aborted.
func SettleAction(life UnitLife, state v1alpha1.ActionState, unit string) (v1alpha1.ActionState, string, bool) {
	switch {
	case life == UnitDying && state == StatePending:
		return StateFailed, fmt.Sprintf("unit %q is dying", unit), true
	case life == UnitGone && state == StatePending:
		return StateCancelled, fmt.Sprintf("unit %q was removed before the action ran", unit), true
	case life == UnitGone && (state == StateRunning || state == StateAborting):
		return StateAborted, fmt.Sprintf("unit %q was removed while the action ran", unit), true
	}
	return "", "", false
}

// ActionReconciler admits Actions (task id, operation id, validation, ownership), settles the ones whose unit goes
// away and removes finished ones after max-action-results-age. The unit's agent runs them.
type ActionReconciler struct {
	client.Client
	// Reader reads without caching; the manager's API reader. Client is used when nil.
	Reader client.Reader
	// Now is time.Now unless a test overrides it.
	Now func() time.Time
}

func (r *ActionReconciler) reader() client.Reader {
	if r.Reader != nil {
		return r.Reader
	}
	return r.Client
}

func (r *ActionReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// Reconcile handles one Action.
func (r *ActionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var act v1alpha1.Action
	if err := r.Get(ctx, req.NamespacedName, &act); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if act.DeletionTimestamp != nil {
		return ctrl.Result{}, nil
	}
	var ns corev1.Namespace
	if err := r.Get(ctx, client.ObjectKey{Name: act.Namespace}, &ns); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if ns.Labels[v1alpha1.ModelLabel] != "true" || ns.DeletionTimestamp != nil {
		return ctrl.Result{}, nil
	}
	if ActionTerminal(act.Status.State) {
		return r.retain(ctx, &act)
	}
	if act.Status.ID == 0 {
		// The cache may lag behind our own write: never hand out a second id.
		var fresh v1alpha1.Action
		if err := r.reader().Get(ctx, req.NamespacedName, &fresh); err != nil {
			return ctrl.Result{}, client.IgnoreNotFound(err)
		}
		if fresh.Status.ID != 0 {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, r.admit(ctx, &act)
	}
	return ctrl.Result{}, r.settle(ctx, &act)
}

// unitLife looks up the Application and the unit's pod and UnitData.
func (r *ActionReconciler) unitLife(ctx context.Context, act *v1alpha1.Action) (*v1alpha1.Application, UnitLife, error) {
	appName, n, ok := ParseUnit(act.Spec.Unit)
	if !ok {
		return nil, UnitGone, nil
	}
	var app v1alpha1.Application
	if err := r.Get(ctx, client.ObjectKey{Namespace: act.Namespace, Name: appName}, &app); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, UnitGone, nil
		}
		return nil, 0, err
	}
	podName := fmt.Sprintf("%s-%d", appName, n)
	exists := func(obj client.Object) (bool, error) {
		err := r.Get(ctx, client.ObjectKey{Namespace: act.Namespace, Name: podName}, obj)
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return err == nil, err
	}
	hasPod, err := exists(&corev1.Pod{})
	if err != nil {
		return nil, 0, err
	}
	hasData, err := exists(&v1alpha1.UnitData{})
	if err != nil {
		return nil, 0, err
	}
	return &app, ClassifyUnit(n, scaleOf(&app), app.DeletionTimestamp != nil, hasPod, hasData), nil
}

// admit gives a new Action its operation label, owner, filled-in parameters, task id and first state.
func (r *ActionReconciler) admit(ctx context.Context, act *v1alpha1.Action) error {
	if err := EnsureModelConfigMap(ctx, r.Client, act.Namespace); err != nil {
		return err
	}
	app, life, err := r.unitLife(ctx, act)
	if err != nil {
		return err
	}
	var verdict ActionAdmission
	appName, _, validUnit := ParseUnit(act.Spec.Unit)
	switch {
	case !validUnit:
		verdict = ActionAdmission{State: StateFailed, Message: fmt.Sprintf("invalid unit %q: want <application>/<number>", act.Spec.Unit)}
	case app == nil:
		verdict = ActionAdmission{State: StateFailed, Message: fmt.Sprintf("application %q not found", appName)}
	default:
		verdict, err = r.admitForApp(act, app, life)
		if err != nil {
			return err
		}
	}

	spec := act.DeepCopy()
	if spec.Labels[v1alpha1.OperationLabel] == "" {
		op, err := AllocateID(ctx, r.reader(), r.Client, act.Namespace, ActionIDKey)
		if err != nil {
			return err
		}
		if spec.Labels == nil {
			spec.Labels = map[string]string{}
		}
		spec.Labels[v1alpha1.OperationLabel] = strconv.FormatInt(op, 10)
	}
	if app != nil && !ownedBy(spec, app) {
		spec.OwnerReferences = append(spec.OwnerReferences, metav1.OwnerReference{
			APIVersion: v1alpha1.GroupVersion.String(), Kind: "Application", Name: app.Name, UID: app.UID,
		})
	}
	if verdict.State == StatePending {
		b, err := json.Marshal(verdict.Parameters)
		if err != nil {
			return err
		}
		spec.Spec.Parameters = &v1alpha1.JSON{Raw: b}
	}
	if !reflect.DeepEqual(spec.ObjectMeta, act.ObjectMeta) || !reflect.DeepEqual(spec.Spec, act.Spec) {
		if err := r.Update(ctx, spec); err != nil {
			return err
		}
	}

	id, err := AllocateID(ctx, r.reader(), r.Client, act.Namespace, ActionIDKey)
	if err != nil {
		return err
	}
	orig := spec.DeepCopy()
	spec.Status.ID, spec.Status.State, spec.Status.Message = id, verdict.State, verdict.Message
	if ActionTerminal(verdict.State) {
		now := metav1.NewTime(r.now())
		spec.Status.Completed = &now
	}
	return r.Status().Patch(ctx, spec, client.MergeFromWithOptions(orig, client.MergeFromWithOptimisticLock{}))
}

func (r *ActionReconciler) admitForApp(act *v1alpha1.Action, app *v1alpha1.Application, life UnitLife) (ActionAdmission, error) {
	var raw []byte
	if app.Status.Charm != nil && app.Status.Charm.Actions != nil {
		raw = app.Status.Charm.Actions.Raw
	}
	if app.Status.Charm == nil || app.Status.Charm.Metadata == nil {
		return ActionAdmission{State: StateFailed, Message: fmt.Sprintf("the charm of application %q is not resolved yet", app.Name)}, nil
	}
	schemas, err := ActionSchemas(raw)
	if err != nil {
		return ActionAdmission{State: StateFailed, Message: err.Error()}, nil
	}
	params := map[string]any{}
	if act.Spec.Parameters != nil && len(act.Spec.Parameters.Raw) > 0 && string(act.Spec.Parameters.Raw) != "null" {
		if err := json.Unmarshal(act.Spec.Parameters.Raw, &params); err != nil {
			return ActionAdmission{State: StateFailed, Message: "validation failed: parameters must be an object"}, nil
		}
	}
	return AdmitAction(life, act.Spec.Unit, act.Spec.Name, params, schemas), nil
}

func ownedBy(o client.Object, app *v1alpha1.Application) bool {
	for _, ref := range o.GetOwnerReferences() {
		if ref.UID == app.UID {
			return true
		}
	}
	return false
}

// settle ends an unfinished Action whose unit is dying or gone.
func (r *ActionReconciler) settle(ctx context.Context, act *v1alpha1.Action) error {
	_, life, err := r.unitLife(ctx, act)
	if err != nil {
		return err
	}
	state, msg, ok := SettleAction(life, act.Status.State, act.Spec.Unit)
	if !ok {
		return nil
	}
	orig := act.DeepCopy()
	now := metav1.NewTime(r.now())
	act.Status.State, act.Status.Message, act.Status.Completed = state, msg, &now
	return r.Status().Patch(ctx, act, client.MergeFromWithOptions(orig, client.MergeFromWithOptimisticLock{}))
}

// retain deletes a finished Action once it is older than max-action-results-age, else looks again then.
func (r *ActionReconciler) retain(ctx context.Context, act *v1alpha1.Action) (ctrl.Result, error) {
	age := DefaultActionResultsAge
	var cm corev1.ConfigMap
	if err := r.Get(ctx, client.ObjectKey{Namespace: act.Namespace, Name: v1alpha1.ModelConfigMap}, &cm); err == nil {
		if d, err := time.ParseDuration(cm.Data[MaxActionResultsAgeKey]); err == nil && d > 0 {
			age = d
		}
	}
	finished := act.CreationTimestamp.Time
	if c := act.Status.Completed; c != nil {
		finished = c.Time
	}
	if left := finished.Add(age).Sub(r.now()); left > 0 {
		return ctrl.Result{RequeueAfter: left + time.Second}, nil
	}
	return ctrl.Result{}, client.IgnoreNotFound(r.Delete(ctx, act))
}

// SetupWithManager registers the controller. Changes of an application or of a unit's pod or UnitData can end the
// unfinished Actions of that application.
func (r *ActionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	forApp := func(ctx context.Context, namespace, app string) []reconcile.Request {
		var acts v1alpha1.ActionList
		if err := r.List(ctx, &acts, client.InNamespace(namespace)); err != nil {
			return nil
		}
		var reqs []reconcile.Request
		for i := range acts.Items {
			a := &acts.Items[i]
			if name, _, ok := ParseUnit(a.Spec.Unit); ok && name == app && !ActionTerminal(a.Status.State) {
				reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: a.Namespace, Name: a.Name}})
			}
		}
		return reqs
	}
	return ctrl.NewControllerManagedBy(mgr).
		Named("action").
		For(&v1alpha1.Action{}).
		Watches(&v1alpha1.Application{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
			return forApp(ctx, o.GetNamespace(), o.GetName())
		})).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
			if app := o.GetLabels()[v1alpha1.AppLabel]; app != "" {
				return forApp(ctx, o.GetNamespace(), app)
			}
			return nil
		})).
		Watches(&v1alpha1.UnitData{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
			if i := strings.LastIndex(o.GetName(), "-"); i > 0 {
				return forApp(ctx, o.GetNamespace(), o.GetName()[:i])
			}
			return nil
		})).
		Complete(r)
}
