package agentenvtest

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/internal/agent"
)

// addApp creates a second application with its Lease, held by its unit 0.
func addApp(t *testing.T, ns, name string) {
	t.Helper()
	ctx := context.Background()
	scale := int32(1)
	if err := c.Create(ctx, &v1alpha1.Application{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: v1alpha1.ApplicationSpec{Charm: v1alpha1.CharmSpec{Name: name}, Scale: &scale}}); err != nil {
		t.Fatal(err)
	}
	dur, holder := int32(60), name+"/0"
	if err := c.Create(ctx, &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: name + "-leader", Namespace: ns},
		Spec: coordinationv1.LeaseSpec{LeaseDurationSeconds: &dur, HolderIdentity: &holder}}); err != nil {
		t.Fatal(err)
	}
}

// appDir is the data dir of unit <app>/0 of a charm with the given metadata.
func appDir(t *testing.T, app, metadata string) *charmDir {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "jkev")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	d := &charmDir{t: t, root: root, agentDir: filepath.Join(root, "agents", "unit-"+app+"-0")}
	d.hookLog = filepath.Join(d.agentDir, "hooks.log")
	d.write("charm/metadata.yaml", metadata)
	d.write("charm/config.yaml", "options: {}\n")
	d.write("charm/dispatch", `#!/bin/sh
A="$CHARM_DIR/.."
case "$JUJU_DISPATCH_PATH" in
actions/*) n="${JUJU_DISPATCH_PATH#actions/}"; [ -f "$A/action-$n" ] && . "$A/action-$n"; exit 0;;
esac
echo "$JUJU_HOOK_NAME" >> "$A/hooks.log"
[ -f "$A/script-$JUJU_HOOK_NAME" ] && . "$A/script-$JUJU_HOOK_NAME"
exit 0
`)
	d.socketDir = filepath.Join(root, "containers")
	return d
}

func appOpts(d *charmDir, ns, app string) agent.RunOptions {
	o := d.opts(ns, 0)
	o.Identity.Pod, o.Identity.App, o.Identity.Containers = app+"-0", app, nil
	return o
}

func unitData(t *testing.T, ns, app string) *v1alpha1.UnitData {
	var ud v1alpha1.UnitData
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: app + "-0"}, &ud); err != nil {
		return nil
	}
	return &ud
}

func inScope(t *testing.T, ns, app, rel string) bool {
	ud := unitData(t, ns, app)
	return ud != nil && ud.Spec.Relations[rel].InScope
}

// Two agents on two applications of one namespace, as the operator leaves them: relation data both ways, an
// application-owned secret granted to the relation and read (and followed) by the other application, an action round
// trip, and the removal handshake with a finalizer held until both units have left the relation.
func TestCrossApplicationRelationSecretActionAndRemoval(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ns, _ := setup(t, "cross", 1)
	setHolder(t, ns, "app/0")
	addApp(t, ns, "db")

	web := appDir(t, "app", "name: web\nrequires:\n  db: {interface: pg}\n")
	db := appDir(t, "db", "name: db\nprovides:\n  database: {interface: pg}\n")

	rel := &v1alpha1.Relation{
		ObjectMeta: metav1.ObjectMeta{Name: "db.database-app.db", Namespace: ns, Finalizers: []string{"test.jk/hold"}},
		Spec: v1alpha1.RelationSpec{Endpoints: []v1alpha1.EndpointRef{
			{Namespace: ns, Application: "db", Endpoint: "database"}, {Namespace: ns, Application: "app", Endpoint: "db"}}},
	}
	if err := c.Create(ctx, rel); err != nil {
		t.Fatal(err)
	}
	rel.Status.ID = 5
	if err := c.Status().Update(ctx, rel); err != nil {
		t.Fatal(err)
	}

	db.write("script-database-relation-joined", `echo "joined $JUJU_REMOTE_UNIT $JUJU_REMOTE_APP" >> "$A/log"
relation-set user=admin
if [ "$(is-leader)" = True ]; then
  relation-set --app role=primary
  id=$(secret-add --label creds password=hunter2)
  secret-grant "$id" -r database:5
  relation-set --app secret-id="$id"
fi
`)
	db.write("script-database-relation-changed", `echo "changed $JUJU_REMOTE_UNIT [$(relation-get greeting $JUJU_REMOTE_UNIT)]" >> "$A/log"`)
	db.write("script-database-relation-departed", `echo "departed $JUJU_REMOTE_UNIT $JUJU_DEPARTING_UNIT" >> "$A/log"`)
	db.write("script-database-relation-broken", `echo "broken $JUJU_REMOTE_APP" >> "$A/log"`)
	web.write("script-db-relation-created", `relation-set greeting=hello`)
	web.write("script-db-relation-changed", `echo "changed [$JUJU_REMOTE_UNIT] $JUJU_REMOTE_APP user=$(relation-get user db/0) role=$(relation-get --app role db) app=$(relation-list --app)" >> "$A/log"
sid=$(relation-get --app secret-id db)
if [ -n "$sid" ]; then secret-get "$sid" --label dbcreds > "$A/secret" 2>&1; fi
`)
	web.write("script-db-relation-departed", `echo "departed $JUJU_REMOTE_UNIT $JUJU_DEPARTING_UNIT" >> "$A/log"`)
	web.write("script-db-relation-broken", `echo "broken $JUJU_REMOTE_APP ids=[$(relation-ids db)]" >> "$A/log"`)
	web.write("script-secret-changed", `echo "secret-changed $JUJU_SECRET_REVISION" >> "$A/log"`)

	runAgent(t, appOpts(db, ns, "db"))
	runAgent(t, appOpts(web, ns, "app"))

	eventually(t, "relation data and secret reach the other side", 90*time.Second, func() bool {
		return strings.Contains(web.read("log"), "user=admin role=primary app=db") && strings.Contains(web.read("secret"), "password: hunter2") &&
			strings.Contains(db.read("log"), "changed app/0 [hello]")
	})
	if !strings.Contains(db.read("log"), "joined app/0 app") {
		t.Fatalf("db log %q", db.read("log"))
	}
	if ud := unitData(t, ns, "app"); ud.Spec.Relations["5"].Endpoint != "db" || ud.Spec.Relations["5"].RemoteApp != "db" ||
		ud.Spec.TrackedSecrets == nil {
		t.Fatalf("%+v", ud.Spec)
	}

	// The owner writes a new revision: the consumer application hears about it.
	var uri string
	ad := &v1alpha1.AppData{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: "db"}, ad); err != nil {
		t.Fatal(err)
	}
	uri = ad.Spec.Relations["5"]["secret-id"]
	db.write("script-update-status", "secret-set "+uri+" password=rotated\n")
	setInterval(t, ns, "1s")
	eventually(t, "secret-changed on the other application", 60*time.Second, func() bool {
		return strings.Contains(web.read("log"), "secret-changed 2")
	})

	// An action round trip on a real API server.
	web.write("action-echo", `action-log "starting"
action-set who="$(action-get who)" id="$JUJU_ACTION_UUID"
echo out-line
`)
	act := &v1alpha1.Action{ObjectMeta: metav1.ObjectMeta{Name: "echo-1", Namespace: ns},
		Spec: v1alpha1.ActionSpec{Unit: "app/0", Name: "echo", Parameters: &v1alpha1.JSON{Raw: []byte(`{"who":"me"}`)}}}
	if err := c.Create(ctx, act); err != nil {
		t.Fatal(err)
	}
	act.Status = v1alpha1.ActionStatus{ID: 42, State: "pending"}
	if err := c.Status().Update(ctx, act); err != nil {
		t.Fatal(err)
	}
	eventually(t, "action completed", 60*time.Second, func() bool {
		_ = c.Get(ctx, client.ObjectKeyFromObject(act), act)
		return act.Status.State == "completed"
	})
	var res map[string]any
	if err := json.Unmarshal(act.Status.Results.Raw, &res); err != nil {
		t.Fatal(err)
	}
	if res["who"] != "me" || res["id"] != "42" || res["stdout"] != "out-line\n" || res["return-code"] != float64(0) ||
		len(act.Status.Log) != 1 || act.Status.Log[0].Message != "starting" || act.Status.Started == nil || act.Status.Completed == nil {
		t.Fatalf("%+v %v", act.Status, res)
	}

	// Removal: the Relation is deleted but held by a finalizer; both units depart and break, then leave scope.
	if err := c.Delete(ctx, rel); err != nil {
		t.Fatal(err)
	}
	eventually(t, "both units out of scope", 60*time.Second, func() bool {
		return !inScope(t, ns, "app", "5") && !inScope(t, ns, "db", "5")
	})
	if !strings.Contains(web.read("log"), "departed db/0 db/0") || !strings.Contains(web.read("log"), "broken db ids=[]") ||
		!strings.Contains(db.read("log"), "departed app/0 app/0") || !strings.Contains(db.read("log"), "broken app") {
		t.Fatalf("web %q\ndb %q", web.read("log"), db.read("log"))
	}
	// The relation-scoped grant went with the relation.
	xid := strings.TrimPrefix(uri, "secret:")
	eventually(t, "relation grant revoked", 30*time.Second, func() bool {
		s, err := (&agent.KubeClient{Reader: c, Client: c, Namespace: ns}).GetSecret(ctx, v1alpha1.SecretMetadataName(xid))
		return err == nil && !strings.Contains(s.Annotations[v1alpha1.SecretGrantsAnnotation], `"relation"`)
	})
	// Release the finalizer: the object goes.
	_ = c.Get(ctx, client.ObjectKeyFromObject(rel), rel)
	rel.Finalizers = nil
	if err := c.Update(ctx, rel); err != nil {
		t.Fatal(err)
	}
}

func setInterval(t *testing.T, ns, d string) {
	t.Helper()
	ctx := context.Background()
	var cm corev1.ConfigMap
	if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: v1alpha1.ModelConfigMap}, &cm); err != nil {
		t.Fatal(err)
	}
	cm.Data = map[string]string{"model-config.update-status-hook-interval": d}
	if err := c.Update(ctx, &cm); err != nil {
		t.Fatal(err)
	}
}
