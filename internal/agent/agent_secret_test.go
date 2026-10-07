package agent

import (
	"encoding/json"
	"regexp"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/internal/hooktools"
)

var uriRE = regexp.MustCompile(`^secret:[0-9a-v]{19}[0g]$`)

// leaderHarness is a started leader unit.
func leaderHarness(t *testing.T) *harness {
	h := peerHarness(t)
	h.makeLeader()
	h.reconcile()
	h.hooks()
	return h
}

func (h *harness) tick() {
	h.clk.Step(6 * time.Minute)
	h.makeLeaderIfWas()
	h.reconcile()
}

func (h *harness) makeLeaderIfWas() {
	if h.a.Leadership().Holder() == "app/0" {
		if _, err := h.a.Leadership().Renew(nil2ctx()); err != nil {
			h.t.Fatal(err)
		}
	}
}

// putSecret creates the Secrets of a secret in the fake API, as another unit would have.
func (h *harness) putSecret(xid string, m SecretMeta, contents map[int]map[string][]byte) {
	h.kube.mu.Lock()
	defer h.kube.mu.Unlock()
	if h.kube.secrets == nil {
		h.kube.secrets = map[string]*corev1.Secret{}
	}
	h.kube.secrets[v1alpha1.SecretMetadataName(xid)] = &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: v1alpha1.SecretMetadataName(xid), Labels: map[string]string{v1alpha1.SecretLabel: xid}, Annotations: m.annotations("")}}
	for rev, c := range contents {
		h.kube.secrets[v1alpha1.SecretRevisionName(xid, rev)] = &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: v1alpha1.SecretRevisionName(xid, rev)}, Data: c}
	}
}

func TestSecretLifecycleOwnedByTheApplication(t *testing.T) {
	t.Parallel()
	h := leaderHarness(t)
	h.write("script-update-status", `secret-add --label db-pass password=s3cret user=admin > "$A/id"
secret-get --label db-pass --format=json > "$A/pending"
`)
	h.tick()
	h.wantHooks("update-status")
	id := strings.TrimSpace(h.read("id"))
	if !uriRE.MatchString(id) || !strings.Contains(h.read("pending"), `"password":"s3cret"`) {
		t.Fatalf("id %q pending %q", id, h.read("pending"))
	}
	xid := strings.TrimPrefix(id, "secret:")
	meta := h.kube.secrets["jk-secret-"+xid]
	rev1 := h.kube.secrets["jk-secret-"+xid+"-1"]
	if meta == nil || rev1 == nil {
		t.Fatalf("secrets %v", h.kube.secretOps)
	}
	if rev1.Immutable == nil || !*rev1.Immutable || string(rev1.Data["password"]) != "s3cret" || rev1.Labels[v1alpha1.AppLabel] != "app" ||
		rev1.Labels[v1alpha1.SecretLabel] != xid || rev1.Labels[v1alpha1.SecretRevisionLabel] != "1" ||
		len(rev1.OwnerReferences) != 1 || rev1.OwnerReferences[0].Kind != "Application" || rev1.OwnerReferences[0].UID != "uid" {
		t.Fatalf("revision secret %+v", rev1)
	}
	m := metaFromSecret(meta)
	if m.OwnerKind != "application" || m.OwnerID != "app" || m.Label != "db-pass" || m.Latest != 1 || len(m.Revisions) != 1 || meta.Labels[v1alpha1.AppLabel] != "app" || meta.Labels[v1alpha1.SecretLabel] != xid {
		t.Fatalf("meta %+v annotations %v", m, meta.Annotations)
	}
	if e := h.kube.appData.OwnedSecrets[xid]; e.Label != "db-pass" || e.Revision != 1 {
		t.Fatalf("index %+v", h.kube.appData.OwnedSecrets)
	}

	// Later hooks: read by label and id, info, ids; a new revision with set; an unchanged value makes none.
	h.write("script-update-status", `secret-get --label db-pass --format=json > "$A/get"
secret-get `+id+` password > "$A/one"
secret-info-get --label db-pass --format=json > "$A/info"
secret-ids > "$A/ids"
secret-set `+id+` password=rotated user=admin
secret-set `+id+` user=admin password=rotated
`)
	// The owner is told about the old revision, which nobody tracks: secret-remove with the revision and the label.
	h.write("script-secret-remove", `echo "$JUJU_SECRET_ID $JUJU_SECRET_REVISION $JUJU_SECRET_LABEL" > "$A/removing"
secret-remove $JUJU_SECRET_ID --revision $JUJU_SECRET_REVISION
`)
	h.tick()
	h.wantHooks("update-status", "secret-remove")
	if !strings.Contains(h.read("get"), `"user":"admin"`) || strings.TrimSpace(h.read("one")) != "s3cret" || strings.TrimSpace(h.read("ids")) != xid ||
		!strings.Contains(h.read("info"), `"revision":1`) || !strings.Contains(h.read("info"), `"owner":"application"`) {
		t.Fatalf("get %q one %q ids %q info %q", h.read("get"), h.read("one"), h.read("ids"), h.read("info"))
	}
	if strings.TrimSpace(h.read("removing")) != id+" 1 db-pass" {
		t.Fatalf("%q", h.read("removing"))
	}
	if h.kube.secrets["jk-secret-"+xid+"-2"] == nil || h.kube.secrets["jk-secret-"+xid+"-3"] != nil {
		t.Fatalf("revisions: %v", h.kube.secretOps)
	}
	if m := metaFromSecret(h.kube.secrets["jk-secret-"+xid]); m.Latest != 2 || len(m.Revisions) != 1 {
		t.Fatalf("%+v", m)
	}
	if e := h.kube.appData.OwnedSecrets[xid]; e.Revision != 2 || len(e.Revisions) != 1 {
		t.Fatalf("index %+v", e)
	}
	if h.kube.secrets["jk-secret-"+xid+"-1"] != nil || h.kube.secrets["jk-secret-"+xid+"-2"] == nil {
		t.Fatalf("revision 1 not removed: %v", h.kube.secretOps)
	}
	h.reconcile()
	h.wantHooks()

	// Removing it altogether deletes every Secret and the index entry.
	h.write("script-update-status", "secret-remove "+id+"\n")
	h.tick()
	h.wantHooks("update-status")
	if len(h.kube.secrets) != 0 || len(h.kube.appData.OwnedSecrets) != 0 {
		t.Fatalf("secrets left: %v %v", h.kube.secretOps, h.kube.appData.OwnedSecrets)
	}
}

func TestSecretOnlyTheLeaderOwnsApplicationSecrets(t *testing.T) {
	t.Parallel()
	h := peerHarness(t)
	h.reconcile()
	h.hooks()
	h.write("script-update-status", `secret-add password=s3cret 2> "$A/err"; echo $? > "$A/code"`)
	h.tick()
	if strings.TrimSpace(h.read("code")) == "0" || !strings.Contains(h.read("err"), "not the leader") || len(h.kube.secrets) != 0 {
		t.Fatalf("code %q err %q", h.read("code"), h.read("err"))
	}
	// A unit-owned secret is fine, and is indexed in the unit's UnitData.
	h.write("script-update-status", `secret-add --owner unit --label mine password=s3cret > "$A/id"`)
	h.tick()
	xid := strings.TrimPrefix(strings.TrimSpace(h.read("id")), "secret:")
	rev := h.kube.secrets["jk-secret-"+xid+"-1"]
	if rev == nil || rev.OwnerReferences[0].Kind != "UnitData" || rev.OwnerReferences[0].Name != "app-0" || h.kube.spec().OwnedSecrets[xid].Label != "mine" {
		t.Fatalf("%+v %v", rev, h.kube.spec().OwnedSecrets)
	}
	if m := metaFromSecret(h.kube.secrets["jk-secret-"+xid]); m.OwnerKind != "unit" || m.OwnerID != "app/0" {
		t.Fatalf("%+v", m)
	}
}

func TestSecretConsumerTracksRevisionsAndGetsSecretChanged(t *testing.T) {
	t.Parallel()
	h := peerHarness(t)
	h.reconcile()
	h.hooks()
	xid := "cnj3q47mp25c7a0e7klg"
	id := "secret:" + xid
	h.putSecret(xid, SecretMeta{XID: xid, OwnerKind: "application", OwnerID: "app", Label: "owner-label", Latest: 1, Revisions: []int{1}},
		map[int]map[string][]byte{1: {"password": []byte("one")}, 2: {"password": []byte("two")}})
	h.kube.appData = &v1alpha1.AppDataSpec{OwnedSecrets: map[string]v1alpha1.OwnedSecret{xid: {Label: "owner-label", Revision: 1, Revisions: []int{1}}}}

	// The first get tracks the latest revision and records the consumer label.
	h.write("script-update-status", `secret-get `+id+` --label owner-label --format=json > "$A/first"`)
	h.tick()
	h.wantHooks("update-status")
	tr := h.kube.spec().TrackedSecrets[id]
	if !strings.Contains(h.read("first"), `"password":"one"`) || tr.Revision != 1 || tr.Label != "" {
		t.Fatalf("first %q tracked %+v", h.read("first"), tr)
	}

	// The owner writes revision 2: this unit hears about it, and keeps reading revision 1 until it refreshes.
	h.kube.appData.OwnedSecrets[xid] = v1alpha1.OwnedSecret{Label: "owner-label", Revision: 2, Revisions: []int{1, 2}}
	h.write("script-secret-changed", `echo "$JUJU_SECRET_ID $JUJU_SECRET_LABEL $JUJU_SECRET_REVISION" > "$A/changed-env"
secret-get --label owner-label > "$A/same"
secret-get --label owner-label --peek --format=json > "$A/peek"
secret-get --label owner-label --format=json > "$A/after-peek"
`)
	h.reconcile()
	h.wantHooks("secret-changed")
	if strings.TrimSpace(h.read("changed-env")) != id+" owner-label 2" || !strings.Contains(h.read("same"), "password: one") ||
		!strings.Contains(h.read("peek"), `"password":"two"`) || !strings.Contains(h.read("after-peek"), `"password":"one"`) {
		t.Fatalf("env %q same %q peek %q after %q", h.read("changed-env"), h.read("same"), h.read("peek"), h.read("after-peek"))
	}
	if h.kube.spec().TrackedSecrets[id].Revision != 2 {
		t.Fatalf("hook secret-changed acknowledges revision 2: %+v", h.kube.spec().TrackedSecrets)
	}
	h.reconcile()
	h.wantHooks()

	// --refresh moves the tracked revision.
	h.kube.appData.OwnedSecrets[xid] = v1alpha1.OwnedSecret{Label: "owner-label", Revision: 3, Revisions: []int{1, 2, 3}}
	h.putSecret(xid, SecretMeta{XID: xid, OwnerKind: "application", OwnerID: "app", Label: "owner-label", Latest: 3, Revisions: []int{1, 2, 3}},
		map[int]map[string][]byte{1: {"password": []byte("one")}, 2: {"password": []byte("two")}, 3: {"password": []byte("three")}})
	h.write("script-secret-changed", `secret-get `+id+` --refresh --format=json > "$A/refreshed"`)
	h.reconcile()
	h.wantHooks("secret-changed")
	if !strings.Contains(h.read("refreshed"), "three") || h.kube.spec().TrackedSecrets[id].Revision != 3 {
		t.Fatalf("%q %+v", h.read("refreshed"), h.kube.spec().TrackedSecrets)
	}

	// The secret is deleted: the tracking record goes.
	h.kube.appData.OwnedSecrets = nil
	for n := range h.kube.secrets {
		delete(h.kube.secrets, n)
	}
	h.reconcile()
	h.wantHooks()
	if _, ok := h.kube.spec().TrackedSecrets[id]; ok {
		t.Fatalf("%+v", h.kube.spec().TrackedSecrets)
	}
}

func TestSecretReadErrors(t *testing.T) {
	t.Parallel()
	h := peerHarness(t)
	h.reconcile()
	h.hooks()
	xid := "cnj3q47mp25c7a0e7klg"
	other := "aaaaaaaaaaaaaaaaaaa0"
	h.putSecret(other, SecretMeta{XID: other, OwnerKind: "application", OwnerID: "elsewhere", Latest: 1, Revisions: []int{1}},
		map[int]map[string][]byte{1: {"password": []byte("x")}})
	h.putSecret(xid, SecretMeta{XID: xid, OwnerKind: "application", OwnerID: "app", Latest: 1, Revisions: []int{1}}, map[int]map[string][]byte{1: {"password": []byte("x")}})
	h.kube.forbidden = map[string]bool{"jk-secret-" + xid + "-1": true}
	h.cfg.SecretRetryDelay = time.Millisecond
	h.restart()
	h.reconcile()
	h.write("script-update-status", `secret-get secret:`+xid+` 2> "$A/forbidden"; echo $? > "$A/c1"
secret-get --label nope 2> "$A/nolabel"
secret-get secret:bbbbbbbbbbbbbbbbbbb0 2> "$A/missing"
secret-get secret:`+other+` 2> "$A/denied"
`)
	h.tick()
	for name, want := range map[string]string{"forbidden": "permission denied", "nolabel": "not found", "missing": "not found", "denied": "permission denied"} {
		if !strings.Contains(h.read(name), want) {
			t.Errorf("%s: %q, want %q", name, h.read(name), want)
		}
	}
	// A grant to the application makes a foreign secret readable.
	g := SecretMeta{XID: other, OwnerKind: "application", OwnerID: "elsewhere", Latest: 1, Revisions: []int{1}, Grants: []v1alpha1.SecretGrant{{Application: "app"}}}
	h.putSecret(other, g, nil)
	h.write("script-update-status", `secret-get secret:`+other+` password > "$A/granted"`)
	h.tick()
	if strings.TrimSpace(h.read("granted")) != "x" {
		t.Fatalf("granted %q", h.read("granted"))
	}
}

func TestSecretExpiredHook(t *testing.T) {
	t.Parallel()
	h := leaderHarness(t)
	h.write("script-update-status", `secret-add --label e --expire 1h token=abc > "$A/id"`)
	h.tick()
	id := strings.TrimSpace(h.read("id"))
	h.write("script-secret-expired", `echo "$JUJU_SECRET_ID $JUJU_SECRET_REVISION $JUJU_SECRET_LABEL" > "$A/expired"`)
	h.hooks()
	if _, ok := h.a.nextSecretExpiry(); !ok {
		t.Fatal("no expiry timer")
	}
	h.reconcile()
	h.wantHooks()
	h.clk.Step(2 * time.Hour)
	h.makeLeader()
	h.reconcile()
	got := h.hooks()
	if len(got) == 0 || got[0] != "secret-expired" || strings.TrimSpace(h.read("expired")) != id+" 1 e" {
		t.Fatalf("hooks %v expired %q", got, h.read("expired"))
	}
	// Once only.
	h.makeLeader()
	h.reconcile()
	for _, hk := range h.hooks() {
		if hk == "secret-expired" {
			t.Fatal("secret-expired ran twice")
		}
	}
}

func TestSecretCommitIsIdempotentAfterACrash(t *testing.T) {
	t.Parallel()
	h := leaderHarness(t)
	h.write("script-update-status", `secret-add --label db-pass password=s3cret >> "$A/ids"`)
	// The agent dies between the secret writes and the AppData/UnitData writes: the hook is retried.
	h.kube.failAppData = 1
	h.tick()
	h.wantHooks("update-status")
	if len(h.kube.secrets) != 2 || h.kube.appData != nil && len(h.kube.appData.OwnedSecrets) != 0 {
		t.Fatalf("first attempt: secrets %v index %+v", h.kube.secretOps, h.kube.appData)
	}
	first := strings.TrimSpace(h.read("ids"))
	h.restart() // a new process: only UnitData (with the execution id) survives
	h.makeLeader()
	h.clk.Step(10 * time.Second)
	h.makeLeader()
	h.reconcile() // retry is due after the backoff
	h.clk.Step(10 * time.Second)
	h.makeLeader()
	h.reconcile()
	ids := strings.Fields(h.read("ids"))
	if len(ids) < 2 || ids[0] != first || ids[len(ids)-1] != first {
		t.Fatalf("ids %v: a rerun must create the same secret", ids)
	}
	if len(h.kube.secrets) != 2 {
		t.Fatalf("duplicate secrets: %v", h.kube.secretOps)
	}
	xid := strings.TrimPrefix(first, "secret:")
	if h.kube.appData.OwnedSecrets[xid].Revision != 1 || h.kube.spec().ErrorState != nil {
		t.Fatalf("index %+v error %v", h.kube.appData.OwnedSecrets, h.kube.spec().ErrorState)
	}
}

func TestSecretUpdateIsIdempotentAfterACrash(t *testing.T) {
	t.Parallel()
	h := leaderHarness(t)
	h.write("script-update-status", `secret-add --label db-pass password=one > "$A/id"`)
	h.tick()
	id := strings.TrimSpace(h.read("id"))
	xid := strings.TrimPrefix(id, "secret:")
	h.write("script-update-status", "secret-set "+id+" password=two\n")
	h.kube.failAppData = 1
	h.tick()
	if h.kube.secrets["jk-secret-"+xid+"-2"] == nil {
		t.Fatal("revision 2 not written before the crash")
	}
	h.restart()
	for i := 0; i < 3; i++ {
		h.clk.Step(10 * time.Second)
		h.makeLeader()
		h.reconcile()
	}
	if h.kube.secrets["jk-secret-"+xid+"-3"] != nil || metaFromSecret(h.kube.secrets["jk-secret-"+xid]).Latest != 2 || h.kube.appData.OwnedSecrets[xid].Revision != 2 {
		t.Fatalf("rerun made another revision: %v %+v", h.kube.secretOps, h.kube.appData.OwnedSecrets)
	}
}

func TestSecretGrantsAndInfoAccess(t *testing.T) {
	t.Parallel()
	h := leaderHarness(t)
	h.write("script-update-status", `secret-add --label g token=abc > "$A/id"`)
	h.tick()
	id := strings.TrimSpace(h.read("id"))
	xid := strings.TrimPrefix(id, "secret:")
	h.write("script-update-status", `secret-grant `+id+` -r database-peers:1
secret-grant `+id+` -r database-peers:1 --unit app/1
secret-info-get `+id+` --format=json > "$A/info"
`)
	h.tick()
	var info map[string]struct {
		Access []hooktools.SecretAccess `json:"access"`
	}
	if err := json.Unmarshal([]byte(h.read("info")), &info); err != nil || len(info[xid].Access) != 1 || info[xid].Access[0].Target != "application-app" {
		t.Fatalf("%v %q", err, h.read("info"))
	}
	if g := metaFromSecret(h.kube.secrets["jk-secret-"+xid]).Grants; len(g) != 1 || g[0].Application != "app" || g[0].Relation != "1" {
		t.Fatalf("grants %+v", g)
	}
	h.write("script-update-status", `secret-revoke `+id+` --app app`)
	h.tick()
	if g := metaFromSecret(h.kube.secrets["jk-secret-"+xid]).Grants; len(g) != 0 {
		t.Fatalf("grants %+v", g)
	}
}

func TestDeriveXID(t *testing.T) {
	t.Parallel()
	a, b := DeriveXID("exec", 0), DeriveXID("exec", 1)
	if a == b || a != DeriveXID("exec", 0) || DeriveXID("other", 0) == a || !regexp.MustCompile(`^[0-9a-v]{19}[0g]$`).MatchString(a) {
		t.Fatalf("%s %s", a, b)
	}
}
