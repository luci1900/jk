package agent

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/luci1900/jk/api/v1alpha1"
)

const peerMetadata = `
name: test
containers:
  app: {resource: app-image}
peers:
  database-peers: {interface: pg}
storage:
  pgdata: {type: filesystem}
`

func peerRelation(id int64) v1alpha1.Relation {
	return v1alpha1.Relation{
		ObjectMeta: metav1.ObjectMeta{Name: "app.database-peers", Namespace: "ns"},
		Spec:       v1alpha1.RelationSpec{Endpoints: []v1alpha1.EndpointRef{{Namespace: "ns", Application: "app", Endpoint: "database-peers"}}},
		Status:     v1alpha1.RelationStatus{ID: id},
	}
}

func peerHarness(t *testing.T) *harness {
	h := newHarness(t, withMetadata(peerMetadata))
	h.kube.relations = []v1alpha1.Relation{peerRelation(1)}
	return h
}

func (h *harness) peer(name string, data map[string]string) {
	if h.kube.others == nil {
		h.kube.others = map[string]*v1alpha1.UnitDataSpec{}
	}
	h.kube.others[name] = &v1alpha1.UnitDataSpec{Relations: map[string]v1alpha1.UnitRelationState{"1": {InScope: true, Data: data}}}
}

func (h *harness) mountStorage() {
	if err := os.MkdirAll(filepath.Join(h.dir, "storage", "pgdata", "0"), 0o755); err != nil {
		h.t.Fatal(err)
	}
}

func TestPeerRelationHooksInJujuOrder(t *testing.T) {
	t.Parallel()
	h := peerHarness(t)
	h.write("script-database-peers-relation-created", `env | grep '^JUJU_' | sort > "$A/created-env"
relation-ids database-peers > "$A/ids-created"
relation-get -r database-peers:1 - app/0 --format=json > "$A/own"
relation-list -r database-peers:1 --format=json > "$A/list-created"
`)
	h.reconcile()
	h.wantHooks("install", "database-peers-relation-created", "config-changed", "start")
	env := h.read("created-env")
	for _, want := range []string{"JUJU_RELATION=database-peers\n", "JUJU_RELATION_ID=database-peers:1\n", "JUJU_REMOTE_APP=app\n", "JUJU_REMOTE_UNIT=\n"} {
		if !strings.Contains(env, want) {
			t.Errorf("missing %q in\n%s", want, env)
		}
	}
	if strings.TrimSpace(h.read("ids-created")) != "database-peers:1" || strings.TrimSpace(h.read("list-created")) != "[]" {
		t.Errorf("ids %q list %q", h.read("ids-created"), h.read("list-created"))
	}
	// The unit's own settings are seeded with its addresses before relation-created runs.
	if got := h.read("own"); !strings.Contains(got, `"ingress-address":"10.1.2.3"`) || !strings.Contains(got, `"private-address":"10.1.2.3"`) || !strings.Contains(got, `"egress-subnets":"10.1.2.3/32"`) {
		t.Errorf("own settings %q", got)
	}
	st := h.kube.spec().Relations["1"]
	if !st.InScope || !st.Created || st.Data["ingress-address"] != "10.1.2.3" {
		t.Fatalf("%+v", st)
	}
}

func TestPeerJoinChangeDepart(t *testing.T) {
	t.Parallel()
	h := peerHarness(t)
	h.reconcile()
	h.hooks()

	h.write("script-database-peers-relation-joined", `echo "$JUJU_REMOTE_UNIT $JUJU_REMOTE_APP" > "$A/joined-env"
relation-list -r database-peers:1 > "$A/joined-list"
`)
	h.write("script-database-peers-relation-changed", `echo "$JUJU_REMOTE_UNIT" >> "$A/changed-units"
relation-get - "$JUJU_REMOTE_UNIT" > "$A/changed-$(echo $JUJU_REMOTE_UNIT | tr / -)"
relation-get unset app/9 --format=json > "$A/missing" 2>&1 || true
`)
	h.peer("app-1", map[string]string{"private-address": "10.9.9.1", "k": "v1"})
	h.peer("app-2", map[string]string{})
	h.reconcile()
	h.wantHooks("database-peers-relation-joined", "database-peers-relation-changed", "database-peers-relation-joined", "database-peers-relation-changed")
	if strings.TrimSpace(h.read("joined-env")) != "app/2 app" {
		t.Errorf("joined env %q", h.read("joined-env"))
	}
	if !strings.Contains(h.read("changed-app-1"), "k: v1") || strings.TrimSpace(h.read("changed-app-2")) != "{}" {
		t.Errorf("app-1 %q app-2 %q", h.read("changed-app-1"), h.read("changed-app-2"))
	}
	if !reflect.DeepEqual(h.kube.spec().Relations["1"].Members, map[string]int64{"app/1": settingsVersion(map[string]string{"private-address": "10.9.9.1", "k": "v1"}), "app/2": 0}) {
		t.Errorf("members %+v", h.kube.spec().Relations["1"].Members)
	}

	// Nothing changed, a restart does not redo anything.
	h.restart()
	h.reconcile()
	h.wantHooks()

	// A peer changes its settings.
	h.peer("app-1", map[string]string{"private-address": "10.9.9.1", "k": "v2"})
	h.reconcile()
	h.wantHooks("database-peers-relation-changed")

	// A peer goes away: departed with the peer as the departing unit, and its last settings still readable.
	h.write("script-database-peers-relation-departed", `echo "$JUJU_REMOTE_UNIT $JUJU_DEPARTING_UNIT" > "$A/departed-env"
relation-list > "$A/departed-list"
relation-get k app/1 > "$A/departed-k"
`)
	delete(h.kube.others, "app-1")
	h.reconcile()
	h.wantHooks("database-peers-relation-departed")
	if strings.TrimSpace(h.read("departed-env")) != "app/1 app/1" || strings.TrimSpace(h.read("departed-list")) != "app/2" || strings.TrimSpace(h.read("departed-k")) != "v2" {
		t.Errorf("env %q list %q k %q", h.read("departed-env"), h.read("departed-list"), h.read("departed-k"))
	}
	if _, ok := h.kube.spec().Relations["1"].Members["app/1"]; ok {
		t.Errorf("%+v", h.kube.spec().Relations["1"])
	}
}

func TestRelationSetCommitsOnSuccessOnly(t *testing.T) {
	t.Parallel()
	h := peerHarness(t)
	h.reconcile()
	h.hooks()
	before := h.kube.spec().Relations["1"].Data
	h.write("script-update-status", `relation-set -r database-peers:1 mykey=myval ingress-address=
relation-get -r database-peers:1 mykey app/0 > "$A/seen"
exit 1
`)
	h.write("fail-update-status", "")
	h.clk.Step(6 * time.Minute)
	h.reconcile()
	h.wantHooks("update-status")
	// The hook failed: its own read saw the change, nothing was committed.
	if strings.TrimSpace(h.read("seen")) != "myval" || !reflect.DeepEqual(h.kube.spec().Relations["1"].Data, before) {
		t.Fatalf("seen %q data %v", h.read("seen"), h.kube.spec().Relations["1"].Data)
	}
	h.remove("fail-update-status")
	h.write("script-update-status", `relation-set -r database-peers:1 mykey=myval ingress-address=
`)
	h.clk.Step(6 * time.Minute)
	h.reconcile()
	h.clk.Step(6 * time.Minute)
	h.reconcile()
	data := h.kube.spec().Relations["1"].Data
	if data["mykey"] != "myval" || data["ingress-address"] != "" || data["private-address"] != "10.1.2.3" {
		t.Fatalf("%v", data)
	}
}

func TestApplicationRelationDataLeaderOnly(t *testing.T) {
	t.Parallel()
	h := peerHarness(t)
	h.reconcile()
	h.hooks()
	h.write("script-update-status", `relation-set --app -r database-peers:1 lkey=lval 2> "$A/err"; echo $? > "$A/code"
`)
	h.clk.Step(6 * time.Minute)
	h.reconcile()
	h.wantHooks("update-status")
	if strings.TrimSpace(h.read("code")) == "0" || !strings.Contains(h.read("err"), "cannot write relation settings") || h.kube.appData != nil && len(h.kube.appData.Relations) > 0 {
		t.Fatalf("code %q err %q", h.read("code"), h.read("err"))
	}
	h.makeLeader()
	h.reconcile()
	h.hooks()
	h.write("script-update-status", `relation-set --app -r database-peers:1 lkey=lval
relation-get --app -r database-peers:1 lkey app > "$A/own-app"
`)
	h.clk.Step(6 * time.Minute)
	h.makeLeader()
	h.reconcile()
	if h.kube.appData == nil {
		t.Fatalf("no app data; hooks %v\n%s", h.hooks(), h.log.String())
	}
	if h.kube.appData.Relations["1"]["lkey"] != "lval" || strings.TrimSpace(h.read("own-app")) != "lval" {
		t.Fatalf("%+v %q", h.kube.appData, h.read("own-app"))
	}
}

func TestApplicationDataChangeRunsApplicationHook(t *testing.T) {
	t.Parallel()
	h := peerHarness(t)
	h.reconcile()
	h.hooks()
	h.write("script-database-peers-relation-changed", `echo "unit=[$JUJU_REMOTE_UNIT] app=[$JUJU_REMOTE_APP]" > "$A/app-hook"
relation-get lkey > "$A/lkey"
`)
	h.kube.appData = &v1alpha1.AppDataSpec{Relations: map[string]v1alpha1.RelationData{"1": {"lkey": "lval"}}}
	h.reconcile()
	h.wantHooks("database-peers-relation-changed")
	if strings.TrimSpace(h.read("app-hook")) != "unit=[] app=[app]" || strings.TrimSpace(h.read("lkey")) != "lval" {
		t.Errorf("%q %q", h.read("app-hook"), h.read("lkey"))
	}
}

func TestRelationScopeRefreshedForNewPodAddress(t *testing.T) {
	t.Parallel()
	h := peerHarness(t)
	h.reconcile()
	h.hooks()
	// The pod is rescheduled with a new IP: the restarted agent updates the seeded settings, so peers see a change.
	h.cfg.PodIP = func() string { return "10.7.7.7" }
	h.restart()
	h.reconcile()
	if d := h.kube.spec().Relations["1"].Data; d["ingress-address"] != "10.7.7.7" || d["egress-subnets"] != "10.7.7.7/32" {
		t.Fatalf("%v", d)
	}
}

func TestDyingUnitDepartsStopsAndLeavesScope(t *testing.T) {
	t.Parallel()
	h := peerHarness(t)
	h.mountStorage()
	h.peer("app-1", map[string]string{"x": "y"})
	h.reconcile()
	h.wantHooks("install", "database-peers-relation-created", "pgdata-storage-attached", "config-changed", "start", "database-peers-relation-joined", "database-peers-relation-changed")
	h.write("script-database-peers-relation-departed", `echo "$JUJU_REMOTE_UNIT $JUJU_DEPARTING_UNIT" >> "$A/departed"`)
	h.write("script-pgdata-storage-detaching", `echo "$JUJU_STORAGE_ID" > "$A/detaching"`)
	h.a.Terminate()
	h.reconcile()
	h.wantHooks("pgdata-storage-detaching", "database-peers-relation-departed", "stop", "remove")
	if strings.TrimSpace(h.read("departed")) != "app/1 app/0" || strings.TrimSpace(h.read("detaching")) != "pgdata/0" {
		t.Errorf("departed %q detaching %q", h.read("departed"), h.read("detaching"))
	}
	// Other units see this one leave: it is out of scope, with its data still there.
	if st := h.kube.spec().Relations["1"]; st.InScope || len(st.Members) != 0 {
		t.Fatalf("%+v", st)
	}
}

func TestStorageHooksAndTools(t *testing.T) {
	t.Parallel()
	h := peerHarness(t)
	h.mountStorage()
	h.write("script-pgdata-storage-attached", `env | grep '^JUJU_STORAGE' | sort > "$A/storage-env"
storage-list > "$A/list"
storage-get -s pgdata/0 location > "$A/loc"
storage-get kind > "$A/kind"
`)
	h.reconcile()
	h.wantHooks("install", "database-peers-relation-created", "pgdata-storage-attached", "config-changed", "start")
	loc := filepath.Join(h.dir, "storage", "pgdata", "0")
	if !strings.Contains(h.read("storage-env"), "JUJU_STORAGE_ID=pgdata/0\n") || !strings.Contains(h.read("storage-env"), "JUJU_STORAGE_LOCATION="+loc+"\n") ||
		strings.TrimSpace(h.read("list")) != "pgdata/0" || strings.TrimSpace(h.read("loc")) != loc || strings.TrimSpace(h.read("kind")) != "filesystem" {
		t.Errorf("env %q list %q loc %q kind %q", h.read("storage-env"), h.read("list"), h.read("loc"), h.read("kind"))
	}
	if !h.kube.spec().StorageAttached["pgdata/0"] {
		t.Fatalf("%v", h.kube.spec().StorageAttached)
	}
	h.restart()
	h.reconcile()
	h.wantHooks()
}

func TestStorageNotMountedYetDoesNotBlockInstall(t *testing.T) {
	t.Parallel()
	h := peerHarness(t)
	h.reconcile()
	h.wantHooks("install", "database-peers-relation-created", "config-changed", "start")
	h.mountStorage()
	h.reconcile()
	h.wantHooks("pgdata-storage-attached")
}

func TestNetworkGetAndGoalState(t *testing.T) {
	t.Parallel()
	h := peerHarness(t)
	h.kube.app.Spec.Scale = ptr(int32(2))
	h.write("script-install", `network-get database-peers --format=json > "$A/net"
goal-state --format=json > "$A/goal"
`)
	h.write("script-start", `goal-state --format=json > "$A/goal2"`)
	h.reconcile()
	h.hooks()
	if !strings.Contains(h.read("net"), `"value":"10.1.2.3"`) || !strings.Contains(h.read("net"), `"ingress-addresses":["10.1.2.3"]`) {
		t.Errorf("net %q", h.read("net"))
	}
	if !strings.Contains(h.read("goal"), `"app/0":{"status":"maintenance"`) || !strings.Contains(h.read("goal"), `"app/1":{"status":"allocating"`) {
		t.Errorf("goal %q", h.read("goal"))
	}
	// A unit above the scale is dying, and so is everything while the application is being removed.
	h.kube.app.Spec.Scale = ptr(int32(1))
	h.peer("app-1", nil)
	h.write("script-database-peers-relation-changed", `goal-state --format=json > "$A/goal3"`)
	h.reconcile()
	if !strings.Contains(h.read("goal3"), `"app/1":{"status":"dying"`) {
		t.Errorf("goal3 %q\n%s", h.read("goal3"), h.log.String())
	}
}

func TestRelationOfAnotherApplicationAndUnknownEndpointsAreIgnored(t *testing.T) {
	t.Parallel()
	h := peerHarness(t)
	other := peerRelation(2)
	other.Name = "other.peers"
	other.Spec.Endpoints = []v1alpha1.EndpointRef{{Namespace: "ns", Application: "other", Endpoint: "database-peers"}}
	unknown := peerRelation(3)
	unknown.Name = "app.unknown"
	unknown.Spec.Endpoints[0].Endpoint = "not-in-metadata"
	unassigned := peerRelation(0)
	unassigned.Name = "app.restart"
	h.kube.relations = append(h.kube.relations, other, unknown, unassigned)
	h.reconcile()
	h.wantHooks("install", "database-peers-relation-created", "config-changed", "start")
}

func TestPeerRelationRestartAtEveryStep(t *testing.T) {
	t.Parallel()
	// The hook sequence does not depend on where the agent restarts.
	seq := func(restartAfter int) []string {
		h := peerHarness(t)
		h.mountStorage()
		h.peer("app-1", map[string]string{"a": "b"})
		var all []string
		for i := 0; ; i++ {
			if restartAfter == i {
				h.restart()
			}
			h.reconcileOne()
			got := h.hooks()
			all = append(all, got...)
			if len(got) == 0 && i > restartAfter+1 {
				return all
			}
			if i > 40 {
				t.Fatalf("no progress: %v", all)
			}
		}
	}
	want := seq(-1)
	if len(want) < 6 {
		t.Fatalf("%v", want)
	}
	for n := 0; n < 4; n++ {
		if got := seq(n); !reflect.DeepEqual(got, want) {
			t.Fatalf("restart at %d: %v, want %v", n, got, want)
		}
	}
}

// reconcileOne reconciles once (a single pass over the resolver is all a restart test needs).
func (h *harness) reconcileOne() { h.reconcile() }

func nil2ctx() context.Context { return context.Background() }
