package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/internal/agent"
)

// Task is one action on one unit (a juju task).
type Task struct {
	ID        int64             `json:"id"`
	Operation int64             `json:"operation"`
	Unit      string            `json:"unit"`
	Action    string            `json:"action"`
	Status    string            `json:"status"`
	Message   string            `json:"message,omitempty"`
	Results   map[string]any    `json:"results,omitempty"`
	Log       []TaskLog         `json:"log,omitempty"`
	Enqueued  time.Time         `json:"enqueued"`
	Started   *time.Time        `json:"started,omitempty"`
	Completed *time.Time        `json:"completed,omitempty"`
	Params    map[string]any    `json:"parameters,omitempty"`
	Labels    map[string]string `json:"-"`
	object    string
}

// TaskLog is one line of `action-log` output.
type TaskLog struct {
	Timestamp time.Time `json:"timestamp"`
	Message   string    `json:"message"`
}

// Terminal says whether the task has finished.
func (t *Task) Terminal() bool {
	switch t.Status {
	case "completed", "failed", "cancelled", "aborted":
		return true
	}
	return false
}

// Operation groups the tasks started by one command.
type Operation struct {
	ID        int64     `json:"id"`
	Summary   string    `json:"summary"`
	Status    string    `json:"status"`
	Enqueued  time.Time `json:"enqueued"`
	Tasks     []*Task   `json:"tasks"`
	Started   *time.Time
	Completed *time.Time
}

// Terminal says whether every task has finished.
func (o *Operation) Terminal() bool {
	for _, t := range o.Tasks {
		if !t.Terminal() {
			return false
		}
	}
	return true
}

// RunOptions describe an action to run.
type RunOptions struct {
	// Units are "app/N" or "app/leader"; Applications run the action on every unit of the application.
	Units        []string
	Applications []string
	// Action is the action's name, or v1alpha1.ActionExec for a command (then Params["command"] is the command line).
	Action  string
	Params  map[string]any
	Timeout time.Duration
}

func taskOf(a *v1alpha1.Action) *Task {
	t := &Task{
		ID: a.Status.ID, Unit: a.Spec.Unit, Action: a.Spec.Name, Status: string(a.Status.State), Message: a.Status.Message,
		Enqueued: a.CreationTimestamp.Time, object: a.Name, Labels: a.Labels,
	}
	t.Operation, _ = strconv.ParseInt(a.Labels[v1alpha1.OperationLabel], 10, 64)
	if a.Status.Started != nil {
		s := a.Status.Started.Time
		t.Started = &s
	}
	if a.Status.Completed != nil {
		s := a.Status.Completed.Time
		t.Completed = &s
	}
	if a.Status.Results != nil {
		_ = json.Unmarshal(a.Status.Results.Raw, &t.Results)
	}
	if a.Spec.Parameters != nil {
		_ = json.Unmarshal(a.Spec.Parameters.Raw, &t.Params)
	}
	for _, l := range a.Status.Log {
		t.Log = append(t.Log, TaskLog{Timestamp: l.Time.Time, Message: l.Message})
	}
	return t
}

// resolveUnits turns the user's unit selectors into unit names.
func (c *Client) resolveUnits(ctx context.Context, o RunOptions) ([]string, error) {
	var out []string
	seen := map[string]bool{}
	add := func(u string) {
		if !seen[u] {
			seen[u] = true
			out = append(out, u)
		}
	}
	for _, u := range o.Units {
		app, rest, ok := strings.Cut(u, "/")
		if !ok || app == "" || rest == "" {
			return nil, fmt.Errorf("invalid unit %q: want <application>/<number> or <application>/leader", u)
		}
		a, err := c.getApplication(ctx, app)
		if err != nil {
			return nil, err
		}
		if rest == "leader" {
			if a.Status.Leader == "" {
				return nil, fmt.Errorf("application %q has no leader yet", app)
			}
			add(a.Status.Leader)
			continue
		}
		if _, _, ok := splitUnit(u); !ok {
			return nil, fmt.Errorf("invalid unit %q: want <application>/<number> or <application>/leader", u)
		}
		add(u)
	}
	for _, app := range o.Applications {
		a, err := c.getApplication(ctx, app)
		if err != nil {
			return nil, err
		}
		scale := 1
		if a.Spec.Scale != nil {
			scale = int(*a.Spec.Scale)
		}
		for n := 0; n < scale; n++ {
			add(unitName(app, n))
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no units to run the action on")
	}
	return out, nil
}

// StartAction creates one Action per unit, all in one operation, and returns once the operator has admitted them
// (tasks that fail validation come back already failed, with the reason in Message).
func (c *Client) StartAction(ctx context.Context, o RunOptions) (*Operation, error) {
	if err := c.requireModel(ctx); err != nil {
		return nil, err
	}
	if o.Action == "" {
		return nil, fmt.Errorf("no action specified")
	}
	units, err := c.resolveUnits(ctx, o)
	if err != nil {
		return nil, err
	}
	if o.Action != v1alpha1.ActionExec {
		for _, u := range units {
			app, _, _ := splitUnit(u)
			acts, err := c.Actions(ctx, app)
			if err != nil {
				return nil, err
			}
			if _, ok := acts[o.Action]; !ok {
				return nil, fmt.Errorf("action %q not defined on unit %q (run `actions %s` to list them)", o.Action, u, app)
			}
		}
	}
	var raw *v1alpha1.JSON
	if len(o.Params) > 0 {
		b, err := json.Marshal(o.Params)
		if err != nil {
			return nil, err
		}
		raw = &v1alpha1.JSON{Raw: b}
	}
	var created []string
	opLabel := ""
	for _, u := range units {
		act := &v1alpha1.Action{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "task-", Namespace: c.Namespace},
			Spec:       v1alpha1.ActionSpec{Unit: u, Name: o.Action, Parameters: raw, TimeoutSeconds: int32(o.Timeout / time.Second)},
		}
		if opLabel != "" {
			act.Labels = map[string]string{v1alpha1.OperationLabel: opLabel}
		}
		if err := c.Kube.Create(ctx, act); err != nil {
			return nil, err
		}
		created = append(created, act.Name)
		if opLabel == "" {
			// The first Action gets the operation id from the operator; the others join it.
			err := c.wait(ctx, 30*time.Second, "the operator to accept the action", func(ctx context.Context) (bool, string, error) {
				if err := c.Kube.Get(ctx, client.ObjectKeyFromObject(act), act); err != nil {
					return false, "", err
				}
				opLabel = act.Labels[v1alpha1.OperationLabel]
				return opLabel != "" && act.Status.State != "", "", nil
			})
			if err != nil {
				return nil, err
			}
		}
	}
	op := &Operation{}
	err = c.wait(ctx, 30*time.Second, "the operator to accept the actions", func(ctx context.Context) (bool, string, error) {
		tasks := make([]*Task, 0, len(created))
		for _, name := range created {
			var a v1alpha1.Action
			if err := c.Kube.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: name}, &a); err != nil {
				return false, "", err
			}
			if a.Status.State == "" {
				return false, "", nil
			}
			tasks = append(tasks, taskOf(&a))
		}
		*op = *newOperation(tasks)
		return true, "", nil
	})
	return op, err
}

// WaitOperation waits until every task of the operation has finished and returns it with its results. A timeout of
// zero waits until ctx is done.
func (c *Client) WaitOperation(ctx context.Context, id int64, timeout time.Duration) (*Operation, error) {
	if timeout == 0 {
		timeout = 24 * time.Hour
	}
	var op *Operation
	err := c.wait(ctx, timeout, fmt.Sprintf("operation %d to finish", id), func(ctx context.Context) (bool, string, error) {
		var err error
		if op, err = c.Operation(ctx, id); err != nil {
			return false, "", err
		}
		return op.Terminal(), "", nil
	})
	return op, err
}

func newOperation(tasks []*Task) *Operation {
	sort.Slice(tasks, func(i, j int) bool { return tasks[i].ID < tasks[j].ID })
	op := &Operation{Tasks: tasks}
	if len(tasks) == 0 {
		return op
	}
	op.ID, op.Enqueued = tasks[0].Operation, tasks[0].Enqueued
	var units []string
	allDone, anyFailed, anyRunning := true, false, false
	for _, t := range tasks {
		units = append(units, t.Unit)
		if t.Enqueued.Before(op.Enqueued) {
			op.Enqueued = t.Enqueued
		}
		if !t.Terminal() {
			allDone = false
		}
		if t.Status == "failed" || t.Status == "aborted" || t.Status == "cancelled" {
			anyFailed = true
		}
		if t.Status == "running" || t.Status == "aborting" {
			anyRunning = true
		}
		if t.Started != nil && (op.Started == nil || t.Started.Before(*op.Started)) {
			s := *t.Started
			op.Started = &s
		}
		if t.Completed != nil && (op.Completed == nil || t.Completed.After(*op.Completed)) {
			s := *t.Completed
			op.Completed = &s
		}
	}
	op.Summary = tasks[0].Action + " run on " + strings.Join(units, ",")
	switch {
	case allDone && anyFailed:
		op.Status = "failed"
	case allDone:
		op.Status = "completed"
	case anyRunning:
		op.Status = "running"
	default:
		op.Status = "pending"
	}
	if !allDone {
		op.Completed = nil
	}
	return op
}

func (c *Client) tasks(ctx context.Context) ([]*Task, error) {
	var list v1alpha1.ActionList
	if err := c.Kube.List(ctx, &list, client.InNamespace(c.Namespace)); err != nil {
		return nil, err
	}
	out := make([]*Task, 0, len(list.Items))
	for i := range list.Items {
		out = append(out, taskOf(&list.Items[i]))
	}
	return out, nil
}

// Operations lists the model's operations, oldest first.
func (c *Client) Operations(ctx context.Context) ([]*Operation, error) {
	ts, err := c.tasks(ctx)
	if err != nil {
		return nil, err
	}
	by := map[int64][]*Task{}
	for _, t := range ts {
		by[t.Operation] = append(by[t.Operation], t)
	}
	var ops []*Operation
	for _, tasks := range by {
		ops = append(ops, newOperation(tasks))
	}
	sort.Slice(ops, func(i, j int) bool { return ops[i].ID < ops[j].ID })
	return ops, nil
}

// Operation returns one operation by id.
func (c *Client) Operation(ctx context.Context, id int64) (*Operation, error) {
	ts, err := c.tasks(ctx)
	if err != nil {
		return nil, err
	}
	var tasks []*Task
	for _, t := range ts {
		if t.Operation == id {
			tasks = append(tasks, t)
		}
	}
	if len(tasks) == 0 {
		return nil, fmt.Errorf("operation %d not found", id)
	}
	return newOperation(tasks), nil
}

// Task returns one task by id.
func (c *Client) Task(ctx context.Context, id int64) (*Task, error) {
	ts, err := c.tasks(ctx)
	if err != nil {
		return nil, err
	}
	for _, t := range ts {
		if t.ID == id {
			return t, nil
		}
	}
	return nil, fmt.Errorf("task %d not found", id)
}

// ParseParams reads `key=value` action parameters: values are YAML, keys may be nested with dots (a.b=c), as in juju.
func ParseParams(args []string) (map[string]any, error) {
	out := map[string]any{}
	for _, a := range args {
		k, v, ok := strings.Cut(a, "=")
		if !ok || k == "" {
			return nil, fmt.Errorf("invalid parameter %q: want key=value", a)
		}
		var val any
		if err := yaml.Unmarshal([]byte(v), &val); err != nil || val == nil {
			val = v
		}
		cur := out
		parts := strings.Split(k, ".")
		for _, p := range parts[:len(parts)-1] {
			next, ok := cur[p].(map[string]any)
			if !ok {
				next = map[string]any{}
				cur[p] = next
			}
			cur = next
		}
		cur[parts[len(parts)-1]] = val
	}
	return out, nil
}

// Resolved tells a unit in a hook error to retry the hook now, or with noRetry to skip it.
func (c *Client) Resolved(ctx context.Context, unit string, noRetry bool) error {
	if _, _, ok := splitUnit(unit); !ok {
		return fmt.Errorf("invalid unit %q: want <application>/<number>", unit)
	}
	mode := "retry"
	if noRetry {
		mode = "no-retry"
	}
	patch := fmt.Sprintf(`{"metadata":{"annotations":{%q:%q}}}`, agent.ResolvedAnnotation, mode)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: c.Namespace, Name: podName(unit)}}
	err := c.Kube.Patch(ctx, pod, client.RawPatch("application/merge-patch+json", []byte(patch)))
	if apierrors.IsNotFound(err) {
		return fmt.Errorf("unit %q not found", unit)
	}
	return err
}
