package sdk

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/luci1900/jk/api/v1alpha1"
)

// readyRelations plays the operator for Relations: valid ones get an id and Ready; names listed in bad are invalid.
func readyRelations(bad map[string]string) func(ctx context.Context, c *Client) {
	return func(ctx context.Context, c *Client) {
		var rels v1alpha1.RelationList
		if err := c.Kube.List(ctx, &rels, client.InNamespace(c.Namespace)); err != nil {
			return
		}
		for i := range rels.Items {
			r := &rels.Items[i]
			if len(r.Status.Conditions) > 0 {
				continue
			}
			cond := metav1.Condition{Type: v1alpha1.RelationReady, Status: metav1.ConditionTrue, Reason: "Ready"}
			if msg, ok := bad[r.Name]; ok {
				cond.Status, cond.Reason, cond.Message = metav1.ConditionFalse, "InterfaceMismatch", msg
			} else {
				r.Status.ID = int64(i + 1)
			}
			meta.SetStatusCondition(&r.Status.Conditions, cond)
			_ = c.Kube.Status().Update(ctx, r)
		}
	}
}

func TestIntegrate(t *testing.T) {
	ctx := context.Background()
	t.Run("infers the pair and creates the relation", func(t *testing.T) {
		c, _ := newClient(t, resolved("pg", "db", dbMetadata, "", "", 1), resolved("web", "web", webMetadata, "", "", 1))
		withOperator(t, c, readyRelations(nil))
		// db provides database (pgsql) and metrics (prometheus_scrape); web requires db and scrape: two pairs.
		_, err := c.Integrate(ctx, "pg", "web")
		wantErr(t, err, "ambiguous relation")
		wantErr(t, err, "pg:database web:db")
		wantErr(t, err, "pg:metrics web:scrape")
		rel, err := c.Integrate(ctx, "pg:database", "web")
		noErr(t, err)
		if rel.Name != "pg.database-web.db" || rel.Spec.Endpoints[0].Application != "pg" || rel.Spec.Endpoints[1].Endpoint != "db" || rel.Status.ID == 0 {
			t.Fatalf("%+v", rel)
		}
		_, err = c.Integrate(ctx, "web:db", "pg:database")
		wantErr(t, err, "already exists")
		_, err = c.Integrate(ctx, "pg", "pg")
		wantErr(t, err, "to itself")
		_, err = c.Integrate(ctx, "pg", "nope")
		wantErr(t, err, `application "nope" not found`)
	})
	t.Run("no compatible endpoints", func(t *testing.T) {
		c, _ := newClient(t, resolved("web", "web", webMetadata, "", "", 1), resolved("other", "other", otherMetadata, "", "", 1))
		_, err := c.Integrate(ctx, "web", "other")
		wantErr(t, err, "no compatible endpoints between web and other")
	})
	t.Run("the limit is checked before creating", func(t *testing.T) {
		c, _ := newClient(t, resolved("pg", "db", dbMetadata, "", "", 1), resolved("pg2", "db", dbMetadata, "", "", 1), resolved("web", "web", webMetadata, "", "", 1))
		withOperator(t, c, readyRelations(nil))
		_, err := c.Integrate(ctx, "pg:database", "web:db")
		noErr(t, err)
		_, err = c.Integrate(ctx, "pg2:database", "web:db")
		wantErr(t, err, "maximum relation limit of 1")
		var rels v1alpha1.RelationList
		noErr(t, c.Kube.List(ctx, &rels, client.InNamespace("m")))
		if len(rels.Items) != 1 {
			t.Fatalf("%d relations", len(rels.Items))
		}
	})
	t.Run("an invalid relation is reported and removed", func(t *testing.T) {
		c, _ := newClient(t, resolved("pg", "db", dbMetadata, "", "", 1), resolved("web", "web", webMetadata, "", "", 1))
		withOperator(t, c, readyRelations(map[string]string{"pg.database-web.db": "the operator disagrees"}))
		_, err := c.Integrate(ctx, "pg:database", "web:db")
		wantErr(t, err, "the operator disagrees")
		var rels v1alpha1.RelationList
		noErr(t, c.Kube.List(ctx, &rels, client.InNamespace("m")))
		if len(rels.Items) != 0 {
			t.Fatalf("the rejected relation is still there")
		}
	})
	t.Run("waits for the charm to be resolved", func(t *testing.T) {
		unresolved := resolved("pg", "db", dbMetadata, "", "", 1)
		unresolved.Status.Charm = nil
		c, _ := newClient(t, unresolved, resolved("web", "web", webMetadata, "", "", 1))
		c.PollInterval = time.Millisecond
		go func() {
			time.Sleep(30 * time.Millisecond)
			good := resolved("pg", "db", dbMetadata, "", "", 1)
			a := &v1alpha1.Application{}
			_ = c.Kube.Get(ctx, client.ObjectKey{Namespace: "m", Name: "pg"}, a)
			a.Status = good.Status
			_ = c.Kube.Status().Update(ctx, a)
		}()
		withOperator(t, c, readyRelations(nil))
		_, err := c.Integrate(ctx, "pg:database", "web:db")
		noErr(t, err)
	})
}

func TestRemoveRelation(t *testing.T) {
	ctx := context.Background()
	rel := func(name, a, ae, b, be string) *v1alpha1.Relation {
		return &v1alpha1.Relation{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "m"}, Spec: v1alpha1.RelationSpec{Endpoints: []v1alpha1.EndpointRef{
			{Namespace: "m", Application: a, Endpoint: ae}, {Namespace: "m", Application: b, Endpoint: be}}}}
	}
	peer := &v1alpha1.Relation{ObjectMeta: metav1.ObjectMeta{Name: "pg.cluster", Namespace: "m"}, Spec: v1alpha1.RelationSpec{Endpoints: []v1alpha1.EndpointRef{{Namespace: "m", Application: "pg", Endpoint: "cluster"}}}}
	c, _ := newClient(t, rel("r1", "pg", "database", "web", "db"), rel("r2", "pg", "metrics", "web", "scrape"), peer)
	wantErr(t, c.RemoveRelation(ctx, "pg", "web"), "ambiguous relation")
	wantErr(t, c.RemoveRelation(ctx, "pg", "other"), "no relation found between pg and other")
	noErr(t, c.RemoveRelation(ctx, "web:db", "pg"))
	var rels v1alpha1.RelationList
	noErr(t, c.Kube.List(ctx, &rels, client.InNamespace("m")))
	if len(rels.Items) != 2 {
		t.Fatalf("%d", len(rels.Items))
	}
	noErr(t, c.RemoveRelation(ctx, "pg", "web"))
	// Peer relations belong to the operator: a one-endpoint relation is never removed here.
	wantErr(t, c.RemoveRelation(ctx, "pg", "pg"), "no relation found")
}

func unitData(name string, workload, agent string, msg string) *v1alpha1.UnitData {
	now := metav1.NewTime(time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC))
	return &v1alpha1.UnitData{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "m"}, Spec: v1alpha1.UnitDataSpec{
		WorkloadStatus:  &v1alpha1.WorkloadStatus{State: workload, Message: msg, Since: &now},
		AgentStatus:     &v1alpha1.WorkloadStatus{State: agent},
		WorkloadVersion: "14.9",
		OpenedPorts:     []v1alpha1.PortRange{{Protocol: "tcp", From: 5432, To: 5432}},
	}}
}

func pod(name, ip string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "m"}, Status: corev1.PodStatus{PodIP: ip}}
}

func TestStatus(t *testing.T) {
	ctx := context.Background()
	pg := resolved("pg", "db", dbMetadata, "", "", 3)
	pg.Status.Leader = "pg/0"
	web := resolved("web", "web", webMetadata, "", "", 1)
	web.Spec.Charm.Source = "local"
	web.Status.Status = &v1alpha1.WorkloadStatus{State: "blocked", Message: "needs db"}
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "pg", Namespace: "m"}, Spec: corev1.ServiceSpec{ClusterIP: "10.0.0.5"}}
	headless := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "m"}, Spec: corev1.ServiceSpec{ClusterIP: "None"}}
	rel := &v1alpha1.Relation{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "m"}, Spec: v1alpha1.RelationSpec{Endpoints: []v1alpha1.EndpointRef{
		{Namespace: "m", Application: "web", Endpoint: "db"}, {Namespace: "m", Application: "pg", Endpoint: "database"}}},
		Status: v1alpha1.RelationStatus{ID: 1, Conditions: []metav1.Condition{{Type: v1alpha1.RelationReady, Status: metav1.ConditionTrue, Reason: "Ready"}, {Type: v1alpha1.RelationValid, Status: metav1.ConditionTrue, Reason: "Valid"}}}}
	peer := &v1alpha1.Relation{ObjectMeta: metav1.ObjectMeta{Name: "pg.cluster", Namespace: "m"}, Spec: v1alpha1.RelationSpec{Endpoints: []v1alpha1.EndpointRef{{Namespace: "m", Application: "pg", Endpoint: "cluster"}}}}
	c, _ := newClient(t, pg, web, svc, headless, rel, peer,
		pod("pg-0", "10.1.0.1"), pod("pg-1", "10.1.0.2"),
		unitData("pg-0", "active", "idle", "Primary"), unitData("pg-1", "maintenance", "executing", "restoring"),
		unitData("web-0", "blocked", "idle", "needs db"), pod("web-0", "10.1.0.9"))
	st, err := c.Status(ctx)
	noErr(t, err)
	if st.Model.Name != "m" || st.Model.Version == "" {
		t.Fatalf("%+v", st.Model)
	}
	p := st.Applications["pg"]
	if p.Charm != "db" || p.CharmRev != 7 || p.CharmChannel != "latest/stable" || p.Scale != 3 || p.Address != "10.0.0.5" || p.Version != "14.9" || p.CharmOrigin != "charmhub" {
		t.Fatalf("%+v", p)
	}
	if len(p.Units) != 3 {
		t.Fatalf("units: %v", p.Units)
	}
	u0, u1, u2 := p.Units["pg/0"], p.Units["pg/1"], p.Units["pg/2"]
	if !u0.Leader || u0.WorkloadStatus.Current != "active" || u0.WorkloadStatus.Message != "Primary" || u0.Address != "10.1.0.1" || u0.OpenedPorts[0] != "5432/TCP" || u0.AgentStatus.Current != "idle" || u0.WorkloadStatus.Since == nil {
		t.Fatalf("%+v", u0)
	}
	if u1.Leader || u1.AgentStatus.Current != "executing" {
		t.Fatalf("%+v", u1)
	}
	// A unit with no pod yet is allocating.
	if u2.WorkloadStatus.Current != "waiting" || u2.AgentStatus.Current != "allocating" || u2.Address != "" {
		t.Fatalf("%+v", u2)
	}
	// With no application status set by the leader, the most urgent unit status stands for the application.
	if p.Status.Current != "waiting" {
		t.Fatalf("app status %+v", p.Status)
	}
	w := st.Applications["web"]
	if w.CharmOrigin != "local" || w.Status.Current != "blocked" || w.Status.Message != "needs db" || w.Address != "" {
		t.Fatalf("%+v", w)
	}
	if len(st.Relations) != 2 {
		t.Fatalf("%+v", st.Relations)
	}
	var reg, peerRel RelationStatus
	for _, r := range st.Relations {
		if r.Type == "peer" {
			peerRel = r
		} else {
			reg = r
		}
	}
	// The provider is listed first whatever the order in the Relation.
	if reg.Provider != "pg:database" || reg.Requirer != "web:db" || reg.Interface != "pgsql" || reg.Status != "joined" {
		t.Fatalf("%+v", reg)
	}
	if peerRel.Provider != "pg:cluster" || peerRel.Interface != "db_peers" {
		t.Fatalf("%+v", peerRel)
	}
	// Not a model.
	c2 := c.InNamespace("kube-system")
	_, err = c2.Status(ctx)
	wantErr(t, err, "not found")
}

func TestStatusAfterScaleDownShowsDyingUnits(t *testing.T) {
	pg := resolved("pg", "db", dbMetadata, "", "", 1)
	c, _ := newClient(t, pg, pod("pg-0", "1.1.1.1"), pod("pg-1", "1.1.1.2"), unitData("pg-0", "active", "idle", ""), unitData("pg-1", "active", "idle", ""))
	st, err := c.Status(context.Background())
	noErr(t, err)
	if len(st.Applications["pg"].Units) != 2 || st.Applications["pg"].Status.Current != "active" {
		t.Fatalf("%+v", st.Applications["pg"])
	}
}

// actionSim admits Actions as the operator does (operation and task ids), and completes them when done is set.
func actionSim(done func(a *v1alpha1.Action) bool) func(ctx context.Context, c *Client) {
	next := int64(0)
	op := int64(0)
	return func(ctx context.Context, c *Client) {
		var acts v1alpha1.ActionList
		if err := c.Kube.List(ctx, &acts, client.InNamespace(c.Namespace)); err != nil {
			return
		}
		for i := range acts.Items {
			a := &acts.Items[i]
			if a.Labels[v1alpha1.OperationLabel] == "" {
				op++
				a.Labels = map[string]string{v1alpha1.OperationLabel: strconv.FormatInt(op, 10)}
				_ = c.Kube.Update(ctx, a)
				continue
			}
			if a.Status.State == "" {
				next++
				a.Status.ID, a.Status.State = next, "pending"
				if a.Spec.Name == "bad" {
					a.Status.State, a.Status.Message = "failed", "invalid parameter"
				}
				_ = c.Kube.Status().Update(ctx, a)
				continue
			}
			if a.Status.State == "pending" && done != nil && done(a) {
				a.Status.State = "completed"
				a.Status.Results = &v1alpha1.JSON{Raw: []byte(fmt.Sprintf(`{"unit":%q,"return-code":0}`, a.Spec.Unit))}
				_ = c.Kube.Status().Update(ctx, a)
			}
		}
	}
}

func TestRunActions(t *testing.T) {
	ctx := context.Background()
	pg := resolved("pg", "db", dbMetadata, dbConfig, dbActions, 2)
	pg.Status.Leader = "pg/1"
	c, _ := newClient(t, pg)
	finish := false
	withOperator(t, c, actionSim(func(*v1alpha1.Action) bool { return finish }))

	op, err := c.StartAction(ctx, RunOptions{Units: []string{"pg/leader"}, Action: "get-password", Params: map[string]any{"username": "x"}})
	noErr(t, err)
	if op.ID != 1 || len(op.Tasks) != 1 || op.Tasks[0].Unit != "pg/1" || op.Tasks[0].Status != "pending" || op.Tasks[0].Params["username"] != "x" {
		t.Fatalf("%+v %+v", op, op.Tasks[0])
	}
	finish = true
	done, err := c.WaitOperation(ctx, op.ID, 5*time.Second)
	noErr(t, err)
	if done.Status != "completed" || done.Tasks[0].Results["unit"] != "pg/1" || done.Summary != "get-password run on pg/1" {
		t.Fatalf("%+v %+v", done, done.Tasks[0])
	}
	// Every unit of an application shares one operation.
	op2, err := c.StartAction(ctx, RunOptions{Applications: []string{"pg"}, Action: "get-password"})
	noErr(t, err)
	if op2.ID != 2 || len(op2.Tasks) != 2 || op2.Tasks[0].Operation != 2 || op2.Tasks[1].Operation != 2 {
		t.Fatalf("%+v", op2)
	}
	if _, err = c.WaitOperation(ctx, op2.ID, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	ops, err := c.Operations(ctx)
	noErr(t, err)
	if len(ops) != 2 || ops[0].ID != 1 || ops[1].ID != 2 || len(ops[1].Tasks) != 2 {
		t.Fatalf("%+v", ops)
	}
	task, err := c.Task(ctx, op2.Tasks[1].ID)
	noErr(t, err)
	if task.Operation != 2 || task.Action != "get-password" {
		t.Fatalf("%+v", task)
	}
	_, err = c.Task(ctx, 99)
	wantErr(t, err, "task 99 not found")
	_, err = c.Operation(ctx, 99)
	wantErr(t, err, "operation 99 not found")
}

func TestRunValidation(t *testing.T) {
	ctx := context.Background()
	pg := resolved("pg", "db", dbMetadata, dbConfig, dbActions, 1)
	c, _ := newClient(t, pg)
	withOperator(t, c, actionSim(nil))
	_, err := c.StartAction(ctx, RunOptions{Units: []string{"pg/0"}, Action: "nope"})
	wantErr(t, err, `action "nope" not defined on unit "pg/0"`)
	_, err = c.StartAction(ctx, RunOptions{Units: []string{"pg"}, Action: "get-password"})
	wantErr(t, err, "invalid unit")
	_, err = c.StartAction(ctx, RunOptions{Units: []string{"pg/x"}, Action: "get-password"})
	wantErr(t, err, "invalid unit")
	_, err = c.StartAction(ctx, RunOptions{Units: []string{"pg/leader"}, Action: "get-password"})
	wantErr(t, err, "no leader yet")
	_, err = c.StartAction(ctx, RunOptions{Units: []string{"nope/0"}, Action: "get-password"})
	wantErr(t, err, "not found")
	_, err = c.StartAction(ctx, RunOptions{Action: "get-password"})
	wantErr(t, err, "no units")
	_, err = c.StartAction(ctx, RunOptions{Units: []string{"pg/0"}})
	wantErr(t, err, "no action specified")
	// exec is not in actions.yaml, and the operator's refusal comes back in the task.
	op, err := c.StartAction(ctx, RunOptions{Units: []string{"pg/0"}, Action: "juju-exec", Params: map[string]any{"command": "ls"}})
	noErr(t, err)
	if op.Tasks[0].Action != "juju-exec" {
		t.Fatalf("%+v", op.Tasks[0])
	}
}

func TestOperationStatusAggregation(t *testing.T) {
	mk := func(states ...string) *Operation {
		var ts []*Task
		for i, s := range states {
			ts = append(ts, &Task{ID: int64(i + 1), Operation: 1, Unit: fmt.Sprintf("a/%d", i), Action: "x", Status: s})
		}
		return newOperation(ts)
	}
	for _, tc := range []struct {
		states []string
		want   string
		term   bool
	}{
		{[]string{"pending"}, "pending", false},
		{[]string{"pending", "running"}, "running", false},
		{[]string{"completed", "running"}, "running", false},
		{[]string{"completed", "completed"}, "completed", true},
		{[]string{"completed", "failed"}, "failed", true},
		{[]string{"aborted"}, "failed", true},
	} {
		op := mk(tc.states...)
		if op.Status != tc.want || op.Terminal() != tc.term {
			t.Errorf("%v: %s terminal=%v", tc.states, op.Status, op.Terminal())
		}
	}
}
