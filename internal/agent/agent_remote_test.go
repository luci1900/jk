package agent

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/luci1900/jk/api/v1alpha1"
)

// offeredRelation is a relation of this unit's application to the application "db" offered by namespace "far", seen
// here as "pg".
func offeredRelation(id int64) v1alpha1.Relation {
	return v1alpha1.Relation{
		ObjectMeta: metav1.ObjectMeta{Name: "app.db-pg", Namespace: "ns"},
		Spec: v1alpha1.RelationSpec{
			Endpoints: []v1alpha1.EndpointRef{{Namespace: "ns", Application: "app", Endpoint: "db"}, {Namespace: "far", Application: "db", Endpoint: "database"}},
			Offer:     "pg-offer", Alias: "pg",
		},
		Status: v1alpha1.RelationStatus{ID: id, RemoteID: 77},
	}
}

func remoteData(id int64, appData map[string]string, units map[string]map[string]string) v1alpha1.RemoteData {
	rd := v1alpha1.RemoteData{
		ObjectMeta: metav1.ObjectMeta{Name: "app.db-pg", Namespace: "ns"},
		Spec:       v1alpha1.RemoteDataSpec{Application: "pg", Source: "far/db", RelationID: id, AppData: appData, Units: map[string]v1alpha1.RemoteUnit{}},
	}
	for u, d := range units {
		rd.Spec.Units[u] = v1alpha1.RemoteUnit{Data: d}
	}
	return rd
}

func TestRelationToAnotherNamespaceUsesTheAlias(t *testing.T) {
	t.Parallel()
	h := newHarness(t, withMetadata(clientMetadata))
	h.makeLeader()
	h.kube.relations = []v1alpha1.Relation{offeredRelation(5)}
	h.kube.remote = []v1alpha1.RemoteData{remoteData(5, map[string]string{"a": "1"}, map[string]map[string]string{
		"pg/0": {"k": "v0", "ingress-address": "10.9.0.1"}, "pg/1": {"k": "v1"},
	})}
	h.write("script-db-relation-created", `echo "$JUJU_REMOTE_APP|$JUJU_REMOTE_UNIT|$JUJU_RELATION_ID" > "$A/created"`)
	h.write("script-db-relation-joined", `echo "$JUJU_REMOTE_APP $JUJU_REMOTE_UNIT" >> "$A/joined"; relation-list > "$A/list"`)
	h.write("script-db-relation-changed", `echo "$JUJU_REMOTE_UNIT|$JUJU_REMOTE_APP" >> "$A/changed"
relation-get --app --format=json > "$A/get-app"
relation-get k "$JUJU_REMOTE_UNIT" >> "$A/k"
relation-set seen="$(relation-get k)"`)
	h.reconcile()
	h.wantHooks("install", "db-relation-created", "leader-elected", "config-changed", "start",
		"db-relation-changed", "db-relation-joined", "db-relation-changed", "db-relation-joined", "db-relation-changed")
	if got := strings.TrimSpace(h.read("created")); got != "pg||db:5" {
		t.Fatalf("created env %q", got)
	}
	if got := strings.Fields(h.read("joined")); len(got) != 4 || got[0] != "pg" || got[1] != "pg/0" || got[3] != "pg/1" {
		t.Fatalf("joined %v", got)
	}
	if got := strings.Fields(h.read("list")); len(got) != 2 || got[0] != "pg/0" || got[1] != "pg/1" {
		t.Fatalf("relation-list %v", got)
	}
	if !strings.Contains(h.read("get-app"), `"a":"1"`) {
		t.Fatalf("application data %q", h.read("get-app"))
	}
	if got := strings.Fields(h.read("k")); len(got) != 2 || got[0] != "v0" || got[1] != "v1" {
		t.Fatalf("remote unit settings %v", got)
	}
	st := h.kube.spec().Relations["5"]
	if st.RemoteApp != "pg" || st.Endpoint != "db" || len(st.Members) != 2 || st.Data["seen"] != "v1" {
		t.Fatalf("%+v", st)
	}

	// The data changes on the other side: a change hook for that unit with the new settings.
	h.write("script-db-relation-changed", `echo "$JUJU_REMOTE_UNIT" > "$A/changed2"; relation-get k "$JUJU_REMOTE_UNIT" > "$A/k2"`)
	h.kube.remote = []v1alpha1.RemoteData{remoteData(5, map[string]string{"a": "1"}, map[string]map[string]string{
		"pg/0": {"k": "v0-new", "ingress-address": "10.9.0.1"}, "pg/1": {"k": "v1"},
	})}
	h.reconcile()
	h.wantHooks("db-relation-changed")
	if strings.TrimSpace(h.read("changed2")) != "pg/0" || strings.TrimSpace(h.read("k2")) != "v0-new" {
		t.Fatalf("%q %q", h.read("changed2"), h.read("k2"))
	}

	// A remote unit leaves: departed with the alias-named unit, its last settings still readable.
	h.write("script-db-relation-departed", `echo "$JUJU_REMOTE_UNIT $JUJU_DEPARTING_UNIT $JUJU_REMOTE_APP" > "$A/departed"; relation-get k pg/1 > "$A/dep-k"`)
	h.kube.remote = []v1alpha1.RemoteData{remoteData(5, map[string]string{"a": "1"}, map[string]map[string]string{"pg/0": {"k": "v0-new", "ingress-address": "10.9.0.1"}})}
	h.reconcile()
	h.wantHooks("db-relation-departed")
	if strings.TrimSpace(h.read("departed")) != "pg/1 pg/1 pg" || strings.TrimSpace(h.read("dep-k")) != "v1" {
		t.Fatalf("%q %q", h.read("departed"), h.read("dep-k"))
	}
}

func TestRemoteDataOfAnotherRelationIsIgnored(t *testing.T) {
	t.Parallel()
	h := newHarness(t, withMetadata(clientMetadata))
	h.kube.relations = []v1alpha1.Relation{offeredRelation(5)}
	// Stale data (another relation id) and data for a relation that does not exist give no remote units.
	stale := remoteData(9, nil, map[string]map[string]string{"pg/0": {"k": "v"}})
	orphan := remoteData(5, nil, map[string]map[string]string{"pg/0": {"k": "v"}})
	orphan.Name = "nothing"
	h.kube.remote = []v1alpha1.RemoteData{stale, orphan}
	h.reconcile()
	h.wantHooks("install", "db-relation-created", "config-changed", "start")
}

func TestApplicationOfTheSameNameInAnotherNamespaceIsTheRemoteSide(t *testing.T) {
	t.Parallel()
	// The offered application is also called "app", like this one: the namespace tells them apart, and the remote
	// side is known by its alias.
	h := newHarness(t, withMetadata(clientMetadata))
	rel := offeredRelation(5)
	rel.Spec.Endpoints[1].Application = "app"
	h.kube.relations = []v1alpha1.Relation{rel}
	h.kube.remote = []v1alpha1.RemoteData{remoteData(5, nil, map[string]map[string]string{"pg/0": {"k": "v"}})}
	h.write("script-db-relation-joined", `echo "$JUJU_REMOTE_APP $JUJU_REMOTE_UNIT" > "$A/joined"`)
	h.reconcile()
	h.wantHooks("install", "db-relation-created", "config-changed", "start", "db-relation-joined", "db-relation-changed")
	if got := strings.TrimSpace(h.read("joined")); got != "pg pg/0" {
		t.Fatalf("%q", got)
	}
}
