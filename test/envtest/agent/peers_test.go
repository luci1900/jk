package agentenvtest

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/internal/agent"
)

func (d *charmDir) read(rel string) string {
	b, _ := os.ReadFile(filepath.Join(d.agentDir, rel))
	return string(b)
}

func createPeerRelation(t *testing.T, ns string, id int64) {
	t.Helper()
	ctx := context.Background()
	rel := &v1alpha1.Relation{
		ObjectMeta: metav1.ObjectMeta{Name: v1alpha1.PeerRelationName("app", "database-peers"), Namespace: ns},
		Spec:       v1alpha1.RelationSpec{Endpoints: []v1alpha1.EndpointRef{{Namespace: ns, Application: "app", Endpoint: "database-peers"}}},
	}
	if err := c.Create(ctx, rel); err != nil {
		t.Fatal(err)
	}
	rel.Status.ID = id
	if err := c.Status().Update(ctx, rel); err != nil {
		t.Fatal(err)
	}
}

func scaleTo(t *testing.T, app *v1alpha1.Application, n int32) {
	t.Helper()
	ctx := context.Background()
	_ = c.Get(ctx, client.ObjectKeyFromObject(app), app)
	app.Spec.Scale = &n
	if err := c.Update(ctx, app); err != nil {
		t.Fatal(err)
	}
}

// Two agents against one API server: each sees the other's unit data and the leader's application data through the
// informer caches, hooks run in juju's order, and a unit that is removed departs from the others.
func TestPeerRelationDataVisibleBetweenAgents(t *testing.T) {
	t.Parallel()
	ns, app := setup(t, "peers", 2)
	setHolder(t, ns, "app/0")
	createPeerRelation(t, ns, 1)

	hooks := func(d *charmDir) string { return strings.Join(d.hooks(), ",") }
	script := func(d *charmDir, n int) {
		d.write("script-database-peers-relation-created", "relation-set -r database-peers:1 who=unit-"+string(rune('0'+n))+"\n[ \"$(is-leader)\" = True ] && relation-set --app -r database-peers:1 leader-says=hello\nexit 0\n")
		d.write("script-database-peers-relation-joined", `echo "$JUJU_REMOTE_UNIT" >> "$A/joined"
relation-list -r database-peers:1 > "$A/list"
`)
		d.write("script-database-peers-relation-changed", `echo "unit=$JUJU_REMOTE_UNIT app=$(relation-get --app leader-says app 2>&1) who=$(relation-get who $JUJU_REMOTE_UNIT 2>&1)" >> "$A/changed"
`)
		d.write("script-database-peers-relation-departed", `echo "$JUJU_REMOTE_UNIT departing=$JUJU_DEPARTING_UNIT who=$(relation-get who $JUJU_REMOTE_UNIT 2>&1)" >> "$A/departed"
`)
	}
	d0, d1 := newCharmDirFor(t, 0), newCharmDirFor(t, 1)
	script(d0, 0)
	script(d1, 1)
	a0 := runAgent(t, d0.opts(ns, 0))
	a1 := runAgent(t, d1.opts(ns, 1))

	eventually(t, "both units see each other", 90*time.Second, func() bool {
		return strings.Contains(d0.read("changed"), "unit=app/1") && strings.Contains(d1.read("changed"), "unit=app/0") &&
			strings.Contains(d1.read("changed"), "app=hello")
	})
	if !strings.Contains(d0.read("changed"), "who=unit-1") || !strings.Contains(d1.read("changed"), "who=unit-0") {
		t.Fatalf("d0 %q d1 %q", d0.read("changed"), d1.read("changed"))
	}
	if strings.TrimSpace(d0.read("list")) != "app/1" || strings.TrimSpace(d1.read("list")) != "app/0" {
		t.Fatalf("lists %q %q", d0.read("list"), d1.read("list"))
	}
	// The leader's hooks: created before leader-elected; the others: created after install.
	if got := hooks(d0); !strings.HasPrefix(got, "install,database-peers-relation-created,leader-elected,config-changed,start") {
		t.Fatalf("leader hooks %s", got)
	}
	if got := hooks(d1); !strings.HasPrefix(got, "install,database-peers-relation-created,config-changed,start") {
		t.Fatalf("unit 1 hooks %s", got)
	}

	// What each agent committed: scope, seeded addresses, versions seen.
	_, ud0 := unitState(t, ns, 0)
	st := ud0.Spec.Relations["1"]
	if !st.InScope || !st.Created || st.Data["who"] != "unit-0" || st.Data["ingress-address"] == "" || st.Data["private-address"] == "" || st.Data["egress-subnets"] == "" ||
		len(st.Members) != 1 {
		t.Fatalf("unit 0 relation state %+v", st)
	}
	ad, _ := kubeFor(ns, 0).AppData(context.Background())
	if ad == nil || ad.Spec.Relations["1"]["leader-says"] != "hello" {
		t.Fatalf("%+v", ad)
	}
	_, ud1 := unitState(t, ns, 1)
	if ud1.Spec.Relations["1"].ApplicationVersion == 0 {
		t.Fatalf("unit 1 has not seen the application data: %+v", ud1.Spec.Relations["1"])
	}

	// A change by one unit is seen by the other.
	d1.write("script-update-status", "relation-set -r database-peers:1 who=unit-1-again\n")
	cm := &corev1.ConfigMap{}
	_ = c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: v1alpha1.ModelConfigMap}, cm)
	cm.Data = map[string]string{"model-config.update-status-hook-interval": "2s"}
	_ = c.Update(context.Background(), cm)
	eventually(t, "unit 0 sees the new data", 60*time.Second, func() bool { return strings.Contains(d0.read("changed"), "who=unit-1-again") })

	// Scale down: unit 1 is terminated and runs departed, stop and remove; unit 0 hears it leave.
	scaleTo(t, app, 1)
	if err := a1.Stop(); err != nil {
		t.Fatal(err)
	}
	tail := d1.hooks()
	if n := len(tail); n < 3 || strings.Join(tail[n-3:], ",") != "database-peers-relation-departed,stop,remove" {
		t.Fatalf("unit 1 hooks: %v", tail)
	}
	if !strings.Contains(d1.read("departed"), "app/0 departing=app/1") {
		t.Fatalf("unit 1 departed: %q", d1.read("departed"))
	}
	eventually(t, "unit 0 departs from unit 1", 30*time.Second, func() bool { return strings.Contains(d0.read("departed"), "app/1 departing=app/1") })
	if !strings.Contains(d0.read("departed"), "who=unit-1") {
		t.Fatalf("settings of a departed unit are still readable: %q", d0.read("departed"))
	}
	_, ud1 = unitState(t, ns, 1)
	if ud1.Spec.Relations["1"].InScope {
		t.Fatalf("unit 1 left scope: %+v", ud1.Spec.Relations["1"])
	}
	_ = a0.Stop()
}

func getSecret(ns, name string) (*corev1.Secret, error) {
	var s corev1.Secret
	err := c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: name}, &s)
	return &s, err
}

// Secrets on a real API server: creation with labels, owner references and immutable revisions; a peer reading the
// application-owned secret and hearing about new revisions; the owner removing a revision nobody tracks any more.
func TestSecretLifecycle(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ns, app := setup(t, "secrets", 2)
	setHolder(t, ns, "app/0")
	createPeerRelation(t, ns, 1)
	d0, d1 := newCharmDirFor(t, 0), newCharmDirFor(t, 1)

	d0.write("script-start", `id=$(secret-add --label peer-pw password=one)
relation-set --app -r database-peers:1 secret-id=$id
state-set secret-id=$id
secret-add --owner unit --label mine private=yes > "$A/unit-secret"
`)
	// The bump flag in the config makes the leader write a new revision.
	d0.write("script-config-changed", `id=$(state-get secret-id)
[ -n "$id" ] && secret-set $id password=two-$(config-get greeting)
exit 0
`)
	d0.write("script-secret-remove", `echo "$JUJU_SECRET_ID $JUJU_SECRET_REVISION $JUJU_SECRET_LABEL" >> "$A/removed"
secret-remove $JUJU_SECRET_ID --revision $JUJU_SECRET_REVISION
`)
	d1.write("script-database-peers-relation-changed", `id=$(relation-get --app secret-id app)
[ -n "$id" ] && secret-get $id --label consumer-label --format=json >> "$A/first"
exit 0
`)
	d1.write("script-secret-changed", `echo "$JUJU_SECRET_ID $JUJU_SECRET_LABEL $JUJU_SECRET_REVISION" >> "$A/changed"
secret-get $JUJU_SECRET_ID --refresh --format=json >> "$A/refreshed"
`)
	runAgent(t, d0.opts(ns, 0))
	runAgent(t, d1.opts(ns, 1))

	eventually(t, "peer reads the secret", 90*time.Second, func() bool { return strings.Contains(d1.read("first"), `"password":"one"`) })
	_, ud0 := unitState(t, ns, 0)
	ad, _ := kubeFor(ns, 0).AppData(ctx)
	id := ad.Spec.Relations["1"]["secret-id"]
	xid := strings.TrimPrefix(id, "secret:")
	if len(xid) != 20 || ad.Spec.OwnedSecrets[xid].Label != "peer-pw" || ad.Spec.OwnedSecrets[xid].Revision != 1 {
		t.Fatalf("id %q index %+v", id, ad.Spec.OwnedSecrets)
	}

	// Objects: an immutable Secret per revision and a metadata Secret, labelled, owned by the Application.
	rev1, err := getSecret(ns, "jk-secret-"+xid+"-1")
	if err != nil {
		t.Fatal(err)
	}
	meta, err := getSecret(ns, "jk-secret-"+xid)
	if err != nil {
		t.Fatal(err)
	}
	if rev1.Immutable == nil || !*rev1.Immutable || string(rev1.Data["password"]) != "one" {
		t.Fatalf("revision secret %+v", rev1)
	}
	for _, s := range []*corev1.Secret{rev1, meta} {
		if s.Labels[v1alpha1.AppLabel] != "app" || s.Labels[v1alpha1.SecretLabel] != xid || len(s.OwnerReferences) != 1 ||
			s.OwnerReferences[0].Kind != "Application" || s.OwnerReferences[0].UID != app.UID {
			t.Fatalf("labels/owners of %s: %v %v", s.Name, s.Labels, s.OwnerReferences)
		}
	}
	if rev1.Labels[v1alpha1.SecretRevisionLabel] != "1" || meta.Annotations[agent.AnnSecretLabel] != "peer-pw" || meta.Annotations[agent.AnnSecretLatest] != "1" {
		t.Fatalf("%v %v", rev1.Labels, meta.Annotations)
	}
	rev1.Data["password"] = []byte("tampered")
	if err := c.Update(ctx, rev1); err == nil {
		t.Fatal("a revision Secret must be immutable")
	}
	// The unit-owned secret is owned by the UnitData.
	uid := strings.TrimPrefix(strings.TrimSpace(d0.read("unit-secret")), "secret:")
	us, err := getSecret(ns, "jk-secret-"+uid+"-1")
	if err != nil || len(us.OwnerReferences) != 1 || us.OwnerReferences[0].Kind != "UnitData" || us.OwnerReferences[0].UID != ud0.UID {
		t.Fatalf("%v %+v", err, us)
	}
	if ud0.Spec.OwnedSecrets[uid].Label != "mine" {
		t.Fatalf("%+v", ud0.Spec.OwnedSecrets)
	}

	// The leader writes revision 2 (config change); the peer gets secret-changed and refreshes; the leader is then
	// told revision 1 is unused and removes it.
	_ = c.Get(ctx, client.ObjectKeyFromObject(app), app)
	app.Spec.Config = map[string]string{"greeting": "hi"}
	if err := c.Update(ctx, app); err != nil {
		t.Fatal(err)
	}
	eventually(t, "secret-changed on the peer", 60*time.Second, func() bool { return strings.Contains(d1.read("refreshed"), `"password":"two-hi"`) })
	if got := strings.TrimSpace(d1.read("changed")); got != id+" peer-pw 2" {
		t.Fatalf("secret-changed env %q", got)
	}
	eventually(t, "revision 1 removed", 60*time.Second, func() bool {
		_, err := getSecret(ns, "jk-secret-"+xid+"-1")
		return apierrors.IsNotFound(err)
	})
	if got := strings.TrimSpace(d0.read("removed")); got != id+" 1 peer-pw" {
		t.Fatalf("secret-remove env %q", got)
	}
	if _, err := getSecret(ns, "jk-secret-"+xid+"-2"); err != nil {
		t.Fatal(err)
	}
	_, ud1 := unitState(t, ns, 1)
	if ud1.Spec.TrackedSecrets[id].Revision != 2 {
		t.Fatalf("%+v", ud1.Spec.TrackedSecrets)
	}
}

// storage-attached runs after install (and leader-elected), storage-get is real, storage-detaching runs before stop.
func TestStorageAttachedAndDetaching(t *testing.T) {
	t.Parallel()
	ns, app := setup(t, "storage", 1)
	setHolder(t, ns, "app/0")
	d := newCharmDir(t)
	if err := os.MkdirAll(filepath.Join(d.root, "storage", "data", "0"), 0o755); err != nil {
		t.Fatal(err)
	}
	d.write("script-data-storage-attached", `storage-get -s data/0 location > "$A/loc"; storage-list > "$A/list"; echo "$JUJU_STORAGE_ID" > "$A/id"`)
	a := runAgent(t, d.opts(ns, 0))
	eventually(t, "started", 60*time.Second, func() bool { st, _ := unitState(t, ns, 0); return st.Started })
	if got := strings.Join(d.hooks(), ","); got != "install,leader-elected,data-storage-attached,config-changed,start" {
		t.Fatalf("%s", got)
	}
	if strings.TrimSpace(d.read("loc")) != filepath.Join(d.root, "storage", "data", "0") || strings.TrimSpace(d.read("list")) != "data/0" || strings.TrimSpace(d.read("id")) != "data/0" {
		t.Fatalf("loc %q list %q id %q", d.read("loc"), d.read("list"), d.read("id"))
	}
	if _, ud := unitState(t, ns, 0); !ud.Spec.StorageAttached["data/0"] {
		t.Fatalf("%+v", ud.Spec.StorageAttached)
	}
	scaleTo(t, app, 0)
	if err := a.Stop(); err != nil {
		t.Fatal(err)
	}
	tail := d.hooks()
	if n := len(tail); n < 3 || strings.Join(tail[n-3:], ",") != "data-storage-detaching,stop,remove" {
		t.Fatalf("%v", tail)
	}
}
