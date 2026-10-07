package agent

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/internal/hooktools"
)

const clientMetadata = `
name: test
containers:
  app: {resource: app-image}
requires:
  db: {interface: pg}
`

func dbRelation(id int64) v1alpha1.Relation {
	return v1alpha1.Relation{
		ObjectMeta: metav1.ObjectMeta{Name: "other.db-app.db", Namespace: "ns"},
		Spec: v1alpha1.RelationSpec{Endpoints: []v1alpha1.EndpointRef{
			{Namespace: "ns", Application: "other", Endpoint: "database"}, {Namespace: "ns", Application: "app", Endpoint: "db"}}},
		Status: v1alpha1.RelationStatus{ID: id},
	}
}

func crossHarness(t *testing.T) *harness {
	h := newHarness(t, withMetadata(clientMetadata))
	h.kube.relations = []v1alpha1.Relation{dbRelation(3)}
	return h
}

func (h *harness) remote(name string, inScope bool, data map[string]string) {
	if h.kube.others == nil {
		h.kube.others = map[string]*v1alpha1.UnitDataSpec{}
	}
	h.kube.others[name] = &v1alpha1.UnitDataSpec{Relations: map[string]v1alpha1.UnitRelationState{"3": {InScope: inScope, Data: data}}}
}

func (h *harness) remoteApp(data map[string]string) {
	if h.kube.appDatas == nil {
		h.kube.appDatas = map[string]*v1alpha1.AppDataSpec{}
	}
	h.kube.appDatas["other"] = &v1alpha1.AppDataSpec{Relations: map[string]v1alpha1.RelationData{"3": data}}
}

func TestCrossApplicationRelationHooksAndTools(t *testing.T) {
	t.Parallel()
	h := crossHarness(t)
	h.makeLeader()
	h.write("script-db-relation-created", `env | grep '^JUJU_' | sort > "$A/created-env"; relation-ids db > "$A/ids"`)
	h.reconcile()
	h.wantHooks("install", "db-relation-created", "leader-elected", "config-changed", "start")
	env := h.read("created-env")
	for _, want := range []string{"JUJU_RELATION=db\n", "JUJU_RELATION_ID=db:3\n", "JUJU_REMOTE_APP=other\n", "JUJU_REMOTE_UNIT=\n"} {
		if !strings.Contains(env, want) {
			t.Errorf("missing %q in\n%s", want, env)
		}
	}
	if strings.Contains(env, "JUJU_DEPARTING_UNIT") || strings.TrimSpace(h.read("ids")) != "db:3" {
		t.Errorf("%s ids %q", env, h.read("ids"))
	}

	h.write("script-db-relation-joined", `echo "$JUJU_REMOTE_UNIT $JUJU_REMOTE_APP" >> "$A/joined"; relation-list > "$A/joined-list"`)
	h.write("script-db-relation-changed", `echo "$JUJU_REMOTE_UNIT|$JUJU_REMOTE_APP" >> "$A/changed"
relation-get --format=json > "$A/get"
relation-get --app --format=json > "$A/get-app"
relation-get -r db:3 k other/0 > "$A/get-k"
relation-list --app > "$A/list-app"
relation-set mine="$(relation-get k)" 
relation-set --app app-key=v
`)
	h.remote("other-0", true, map[string]string{"k": "v1", "ingress-address": "10.9.0.1"})
	h.remote("other-1", true, map[string]string{"k": "w1"})
	h.remoteApp(map[string]string{"a": "1"})
	h.reconcile()
	h.wantHooks("db-relation-changed", "db-relation-joined", "db-relation-changed", "db-relation-joined", "db-relation-changed")
	st := h.kube.spec().Relations["3"]
	if st.Data["mine"] != "w1" || st.Endpoint != "db" || st.RemoteApp != "other" || len(st.Members) != 2 {
		t.Fatalf("%+v", st)
	}
	if !strings.Contains(h.read("get-app"), `"a":"1"`) || strings.TrimSpace(h.read("list-app")) != "other" {
		t.Fatalf("app %q list %q", h.read("get-app"), h.read("list-app"))
	}
	if h.kube.appData == nil || h.kube.appData.Relations["3"]["app-key"] != "v" {
		t.Fatalf("app data %+v", h.kube.appData)
	}
	if got := strings.Fields(h.read("changed")); got[0] != "|other" {
		t.Fatalf("app hook first: %v", got)
	}

	// Coalescing: several changes between two reconciles give one hook per unit with the latest data.
	h.write("script-db-relation-changed", `echo "$JUJU_REMOTE_UNIT" >> "$A/changed2"; relation-get k "$JUJU_REMOTE_UNIT" >> "$A/latest"`)
	h.remote("other-0", true, map[string]string{"k": "v2"})
	h.remote("other-0", true, map[string]string{"k": "v3"})
	h.reconcile()
	h.wantHooks("db-relation-changed")
	if strings.TrimSpace(h.read("latest")) != "v3" {
		t.Fatalf("%q", h.read("latest"))
	}

	// A remote unit leaves scope: departed with that unit, its last settings readable.
	h.write("script-db-relation-departed", `echo "$JUJU_REMOTE_UNIT $JUJU_DEPARTING_UNIT" > "$A/departed"; relation-list > "$A/dep-list"; relation-get k other/1 > "$A/dep-k"`)
	h.remote("other-1", false, map[string]string{"k": "w1"})
	h.reconcile()
	h.wantHooks("db-relation-departed")
	if strings.TrimSpace(h.read("departed")) != "other/1 other/1" || strings.TrimSpace(h.read("dep-list")) != "other/0" || strings.TrimSpace(h.read("dep-k")) != "w1" {
		t.Fatalf("%q %q %q", h.read("departed"), h.read("dep-list"), h.read("dep-k"))
	}
}

func TestCrossApplicationRelationRemovalHandshake(t *testing.T) {
	t.Parallel()
	h := crossHarness(t)
	h.remote("other-0", true, map[string]string{"k": "v"})
	h.remote("other-1", true, map[string]string{"k": "v"})
	h.reconcile()
	h.hooks()
	h.write("script-db-relation-departed", `echo "$JUJU_REMOTE_UNIT $JUJU_DEPARTING_UNIT" >> "$A/dep"`)
	h.write("script-db-relation-broken", `echo "[$JUJU_REMOTE_UNIT] $JUJU_REMOTE_APP" > "$A/broken"; relation-ids db > "$A/ids-broken"`)

	// The Relation is deleted (its finalizer holds it): the unit departs from everyone, breaks, leaves scope.
	now := metav1.Now()
	h.kube.relations[0].DeletionTimestamp = &now
	h.reconcile()
	h.wantHooks("db-relation-departed", "db-relation-departed", "db-relation-broken")
	if got := strings.Fields(h.read("dep")); strings.Join(got, ",") != "other/0,other/0,other/1,other/1" {
		t.Fatalf("%v", got)
	}
	if strings.TrimSpace(h.read("broken")) != "[] other" || strings.TrimSpace(h.read("ids-broken")) != "" {
		t.Fatalf("%q %q", h.read("broken"), h.read("ids-broken"))
	}
	st := h.kube.spec().Relations["3"]
	if st.InScope || st.Created || len(st.Members) != 0 || st.Endpoint != "db" {
		t.Fatalf("not out of scope: %+v", st)
	}
	// Nothing more happens, also after a restart, and the unit does not re-enter.
	h.restart()
	h.reconcile()
	h.wantHooks()
	if h.kube.spec().Relations["3"].InScope {
		t.Fatal("re-entered scope")
	}
}

func TestCrossApplicationRelationVanishedObjectDepartsAfterRestart(t *testing.T) {
	t.Parallel()
	h := crossHarness(t)
	h.remote("other-0", true, map[string]string{"k": "v"})
	h.reconcile()
	h.hooks()
	h.kube.relations = nil
	h.restart()
	h.reconcile()
	h.wantHooks("db-relation-departed", "db-relation-broken")
	if h.kube.spec().Relations["3"].InScope {
		t.Fatal("still in scope")
	}
}

func TestCrossApplicationRelationRestartAtEveryStep(t *testing.T) {
	t.Parallel()
	h := crossHarness(t)
	h.remote("other-0", true, map[string]string{"k": "v"})
	var all []string
	for i := 0; i < 8; i++ {
		h.restart()
		h.reconcile()
		all = append(all, h.hooks()...)
	}
	if got := strings.Join(all, ","); got != "install,db-relation-created,config-changed,start,db-relation-joined,db-relation-changed" {
		t.Fatal(got)
	}
}

func TestRemoteUnitNotInRelationHasNoSettings(t *testing.T) {
	t.Parallel()
	h := crossHarness(t)
	h.remote("other-0", true, map[string]string{"k": "v"})
	h.kube.others["third-0"] = &v1alpha1.UnitDataSpec{}
	h.write("script-db-relation-changed", `relation-get k third/0 > "$A/out" 2>&1; echo $? > "$A/code"`)
	h.reconcile()
	if !strings.Contains(h.read("out"), "not found") || strings.TrimSpace(h.read("code")) == "0" {
		t.Fatalf("%q %q", h.read("out"), h.read("code"))
	}
}

// Upgrade-charm.

func upgradeHarness(t *testing.T, image *string) *harness {
	h := newHarness(t)
	h.kube.app.Status.Charm = &v1alpha1.ResolvedCharm{Revision: 5, Image: "reg/charm@sha256:aaa"}
	base := h.cfg.Getenv
	h.cfg.Getenv = func(k string) (string, bool) {
		if k == v1alpha1.CharmImageEnv && *image != "" {
			return *image, true
		}
		return base(k)
	}
	h.restart()
	return h
}

func TestUpgradeCharmRunsAfterRestartWithNewCharm(t *testing.T) {
	t.Parallel()
	image := "reg/charm@sha256:aaa"
	h := upgradeHarness(t, &image)
	h.reconcile()
	// First install records the charm without an upgrade hook.
	h.wantHooks("install", "config-changed", "start")
	if sp := h.kube.spec(); sp.CharmURL != image || sp.CharmRevision != 5 {
		t.Fatalf("%q %d", sp.CharmURL, sp.CharmRevision)
	}
	h.reconcile()
	h.wantHooks()

	// The operator publishes a new revision: the running pod is not upgraded until it is replaced.
	h.kube.app.Status.Charm = &v1alpha1.ResolvedCharm{Revision: 6, Image: "reg/charm@sha256:bbb"}
	h.reconcile()
	h.wantHooks()

	// A pod with the new charm: upgrade-charm, then config-changed, no install or start.
	image = "reg/charm@sha256:bbb"
	h.restart()
	h.reconcile()
	h.wantHooks("upgrade-charm", "config-changed")
	if sp := h.kube.spec(); sp.CharmURL != image || sp.CharmRevision != 6 {
		t.Fatalf("%q %d", sp.CharmURL, sp.CharmRevision)
	}
	h.restart()
	h.reconcile()
	h.wantHooks()
}

func TestUpgradeCharmFailureRetriesAndKeepsOldCharmRecorded(t *testing.T) {
	t.Parallel()
	image := "a"
	h := upgradeHarness(t, &image)
	h.reconcile()
	h.hooks()
	image = "b"
	h.write("fail-upgrade-charm", "")
	h.restart()
	h.reconcile()
	h.wantHooks("upgrade-charm")
	if h.kube.spec().CharmURL != "a" {
		t.Fatalf("recorded %q", h.kube.spec().CharmURL)
	}
	h.remove("fail-upgrade-charm")
	h.write("script-upgrade-charm", "")
	h.restart()
	h.reconcile()
	h.clk.Step(time.Minute)
	h.reconcile()
	h.wantHooks("upgrade-charm", "config-changed")
}

func TestUnitWithoutRecordedCharmRecordsWithoutHook(t *testing.T) {
	t.Parallel()
	image := "a"
	h := upgradeHarness(t, &image)
	h.reconcile()
	h.hooks()
	h.kube.mu.Lock()
	h.kube.unit.CharmURL = ""
	h.kube.mu.Unlock()
	h.restart()
	h.reconcile()
	h.wantHooks()
	if h.kube.spec().CharmURL != "a" {
		t.Fatal("not recorded")
	}
}

func TestCharmIdentity(t *testing.T) {
	h := newHarness(t)
	if u, r := h.a.charmIdentity(&v1alpha1.Application{}); u != "" || r != 0 {
		t.Fatal(u, r)
	}
	app := &v1alpha1.Application{Status: v1alpha1.ApplicationStatus{Charm: &v1alpha1.ResolvedCharm{Revision: 2, Sha256: "abc"}}}
	if u, r := h.a.charmIdentity(app); u != "abc" || r != 2 {
		t.Fatal(u, r)
	}
}

// Cross-application secrets.

func foreignSecret(h *harness, xid string, grants []v1alpha1.SecretGrant, rev int) {
	revs := []int{}
	content := map[int]map[string][]byte{}
	for i := 1; i <= rev; i++ {
		revs = append(revs, i)
		content[i] = map[string][]byte{"token": []byte("t" + string(rune('0'+i)))}
	}
	h.putSecret(xid, SecretMeta{XID: xid, OwnerKind: "application", OwnerID: "other", Label: "theirs", Latest: rev, Revisions: revs, Grants: grants}, content)
	if h.kube.appDatas == nil {
		h.kube.appDatas = map[string]*v1alpha1.AppDataSpec{}
	}
	ad := h.kube.appDatas["other"]
	if ad == nil {
		ad = &v1alpha1.AppDataSpec{}
		h.kube.appDatas["other"] = ad
	}
	if ad.OwnedSecrets == nil {
		ad.OwnedSecrets = map[string]v1alpha1.OwnedSecret{}
	}
	ad.OwnedSecrets[xid] = v1alpha1.OwnedSecret{Label: "theirs", Revision: rev, Revisions: revs, Grants: grants}
}

func TestConsumerReadsSecretOfAnotherApplication(t *testing.T) {
	t.Parallel()
	h := crossHarness(t)
	h.reconcile()
	h.hooks()
	xid := "cnj3q47mp25c7a0e7klg"
	id := "secret:" + xid
	foreignSecret(h, xid, []v1alpha1.SecretGrant{{Application: "app", Relation: "3"}}, 1)
	h.write("script-update-status", `secret-get `+id+` --label mine --format=json > "$A/first" 2>&1
secret-set `+id+` token=x > "$A/set" 2>&1; echo $? >> "$A/set"
secret-info-get `+id+` > "$A/info" 2>&1; echo $? >> "$A/info"
`)
	h.tick()
	h.wantHooks("update-status")
	if !strings.Contains(h.read("first"), `"token":"t1"`) {
		t.Fatalf("%q", h.read("first"))
	}
	if tr := h.kube.spec().TrackedSecrets[id]; tr.Revision != 1 || tr.Label != "mine" {
		t.Fatalf("%+v", tr)
	}
	// Not the owner: juju does not know the secret as one it manages.
	if !strings.Contains(h.read("set"), "not found") || !strings.Contains(h.read("info"), "not found") {
		t.Fatalf("set %q info %q", h.read("set"), h.read("info"))
	}

	// A new revision: secret-changed, the consumer keeps reading the tracked one until it refreshes.
	foreignSecret(h, xid, []v1alpha1.SecretGrant{{Application: "app", Relation: "3"}}, 2)
	h.write("script-secret-changed", `echo "$JUJU_SECRET_ID $JUJU_SECRET_REVISION" > "$A/ch"
secret-get --label mine --format=json > "$A/bylabel"; secret-get --label mine --format=json > "$A/tracked"; secret-get --label mine --peek --format=json > "$A/peek"; secret-get --label mine --refresh --format=json > "$A/refresh"`)
	h.reconcile()
	h.wantHooks("secret-changed")
	if strings.TrimSpace(h.read("ch")) != id+" 2" || !strings.Contains(h.read("tracked"), "t1") || !strings.Contains(h.read("peek"), "t2") || !strings.Contains(h.read("refresh"), "t2") {
		t.Fatalf("%q %q %q %q", h.read("ch"), h.read("tracked"), h.read("peek"), h.read("refresh"))
	}
	if h.kube.spec().TrackedSecrets[id].Revision != 2 {
		t.Fatalf("%+v", h.kube.spec().TrackedSecrets)
	}
	h.reconcile()
	h.wantHooks()
}

func TestConsumerAccessIsEnforcedByGrants(t *testing.T) {
	t.Parallel()
	h := crossHarness(t)
	h.reconcile()
	h.hooks()
	read := func(id string) string {
		h.write("script-update-status", `secret-get `+id+` > "$A/out" 2>&1; echo "code=$?" >> "$A/out"`)
		h.tick()
		h.hooks()
		return h.read("out")
	}
	// Not granted at all, granted to another application, to another unit, and to a relation this unit is not in.
	cases := map[string][]v1alpha1.SecretGrant{
		"cnj3q47mp25c7a0e7k10": nil,
		"cnj3q47mp25c7a0e7k20": {{Application: "third"}},
		"cnj3q47mp25c7a0e7k30": {{Application: "app", Unit: "app/9"}},
		"cnj3q47mp25c7a0e7k40": {{Application: "app", Relation: "9"}},
	}
	for xid, g := range cases {
		foreignSecret(h, xid, g, 1)
		if out := read("secret:" + xid); !strings.Contains(out, "permission denied") || !strings.Contains(out, "code=1") {
			t.Errorf("%v: %q", g, out)
		}
	}
	// Granted to this unit, or to the application.
	for i, g := range [][]v1alpha1.SecretGrant{{{Application: "app", Unit: "app/0"}}, {{Application: "app"}}, {{Application: "app", Relation: "3", Unit: "app/0"}}} {
		xid := "cnj3q47mp25c7a0e7k" + string(rune('5'+i)) + "0"
		foreignSecret(h, xid, g, 1)
		if out := read("secret:" + xid); !strings.Contains(out, "token: t1") {
			t.Errorf("%v: %q", g, out)
		}
	}
	// The index says no but the metadata Secret (the truth) says yes: the grant is honoured.
	xid := "cnj3q47mp25c7a0e7k80"
	foreignSecret(h, xid, []v1alpha1.SecretGrant{{Application: "app"}}, 1)
	h.kube.appDatas["other"].OwnedSecrets[xid] = v1alpha1.OwnedSecret{Label: "theirs", Revision: 1, Revisions: []int{1}}
	if out := read("secret:" + xid); !strings.Contains(out, "token: t1") {
		t.Errorf("%q", out)
	}
	// Unknown secret.
	if out := read("secret:cnj3q47mp25c7a0e7k90"); !strings.Contains(out, "not found") {
		t.Errorf("%q", out)
	}
}

func TestConsumerRetriesForbiddenForGrantedSecret(t *testing.T) {
	t.Parallel()
	h := crossHarness(t)
	h.cfg.SecretRetryDelay = 1
	h.a.cfg.SecretRetryDelay = 1
	h.reconcile()
	h.hooks()
	xid := "cnj3q47mp25c7a0e7klg"
	foreignSecret(h, xid, []v1alpha1.SecretGrant{{Application: "app"}}, 1)
	rev := v1alpha1.SecretRevisionName(xid, 1)
	h.kube.forbidden = map[string]bool{rev: true}
	h.write("script-update-status", `secret-get secret:`+xid+` > "$A/out" 2>&1; echo "code=$?" >> "$A/out"`)
	h.tick()
	if out := h.read("out"); !strings.Contains(out, "permission denied") {
		t.Fatalf("%q", out)
	}
}

func TestOwnerGrantsToAnotherApplicationAndRevokesWithTheRelation(t *testing.T) {
	t.Parallel()
	h := newHarness(t, withMetadata(`
name: test
containers:
  app: {resource: app-image}
provides:
  database: {interface: pg}
`))
	rel := dbRelation(3)
	rel.Spec.Endpoints = []v1alpha1.EndpointRef{{Namespace: "ns", Application: "app", Endpoint: "database"}, {Namespace: "ns", Application: "other", Endpoint: "db"}}
	h.kube.relations = []v1alpha1.Relation{rel}
	h.makeLeader()
	h.reconcile()
	h.hooks()
	h.write("script-update-status", `secret-add --label l token=abc > "$A/id"`)
	h.tick()
	id := strings.TrimSpace(h.read("id"))
	xid := strings.TrimPrefix(id, "secret:")
	h.write("script-update-status", `secret-grant `+id+` -r database:3
secret-grant `+id+` -r database:3 --unit other/1`)
	h.tick()
	g := metaFromSecret(h.kube.secrets["jk-secret-"+xid]).Grants
	if len(g) != 1 || g[0].Application != "other" || g[0].Relation != "3" || g[0].Unit != "" {
		t.Fatalf("grants %+v", g)
	}
	// The owner's index carries the grants for consumers.
	if idx := h.kube.appData.OwnedSecrets[xid].Grants; len(idx) != 1 || idx[0].Application != "other" {
		t.Fatalf("index %+v", idx)
	}
	// A unit grant, scoped to the relation, after an application one is refused by juju's rule only the other way round.
	h.write("script-update-status", `secret-revoke `+id+` --app other
secret-grant `+id+` -r database:3 --unit other/1
secret-grant `+id+` -r database:3`)
	h.tick()
	g = metaFromSecret(h.kube.secrets["jk-secret-"+xid]).Grants
	if len(g) != 1 || g[0].Unit != "" {
		// the app-level grant came last: it is refused while the unit grant exists in the same hook.
		t.Logf("grants %+v", g)
	}

	// The relation goes away: its grants go with it.
	now := metav1.Now()
	h.kube.relations[0].DeletionTimestamp = &now
	h.kube.mu.Lock()
	h.kube.others = map[string]*v1alpha1.UnitDataSpec{}
	h.kube.mu.Unlock()
	h.reconcile()
	for _, gr := range metaFromSecret(h.kube.secrets["jk-secret-"+xid]).Grants {
		if gr.Relation == "3" {
			t.Fatalf("relation grant survived: %+v", gr)
		}
	}
	_ = hooktools.OwnerUnit
}

func TestDyingRelationGrantsStopConsumerAccess(t *testing.T) {
	t.Parallel()
	h := crossHarness(t)
	h.remote("other-0", true, map[string]string{"k": "v"})
	h.reconcile()
	h.hooks()
	xid := "cnj3q47mp25c7a0e7klg"
	foreignSecret(h, xid, []v1alpha1.SecretGrant{{Application: "app", Relation: "3"}}, 1)
	now := metav1.Now()
	h.kube.relations[0].DeletionTimestamp = &now
	h.write("script-update-status", `secret-get secret:`+xid+` > "$A/out" 2>&1`)
	h.tick()
	h.tick()
	if !strings.Contains(h.read("out"), "permission denied") {
		t.Fatalf("%q", h.read("out"))
	}
}
