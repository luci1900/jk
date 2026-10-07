package agent

import (
	"strings"
	"testing"
	"time"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/internal/hooktools"
)

func TestSecretWriteGuards(t *testing.T) {
	t.Parallel()
	h := peerHarness(t)
	h.reconcile()
	h.hooks()
	xid := "cnj3q47mp25c7a0e7klg"
	id := "secret:" + xid
	h.putSecret(xid, SecretMeta{XID: xid, OwnerKind: "application", OwnerID: "app", Label: "l", Latest: 1, Revisions: []int{1}}, map[int]map[string][]byte{1: {"password": []byte("x")}})
	h.kube.appData = &v1alpha1.AppDataSpec{OwnedSecrets: map[string]v1alpha1.OwnedSecret{xid: {Label: "l", Revision: 1, Revisions: []int{1}}}}
	// Non-leaders cannot change, remove or grant application-owned secrets, and do not see them as theirs.
	h.write("script-update-status", `secret-set `+id+` password=y 2> "$A/set"
secret-remove `+id+` 2> "$A/remove"
secret-grant `+id+` -r 1 2> "$A/grant"
secret-set secret:bbbbbbbbbbbbbbbbbbb0 password=y 2> "$A/unknown"
secret-ids > "$A/ids"
secret-info-get --label l 2> "$A/info"
`)
	h.tick()
	for _, f := range []string{"set", "remove", "grant"} {
		if !strings.Contains(h.read(f), "not the leader") {
			t.Errorf("%s: %q", f, h.read(f))
		}
	}
	if !strings.Contains(h.read("unknown"), "not found") || strings.TrimSpace(h.read("ids")) != "" || !strings.Contains(h.read("info"), "not found") {
		t.Errorf("unknown %q ids %q info %q", h.read("unknown"), h.read("ids"), h.read("info"))
	}
	if len(h.kube.secretOps) != 0 {
		t.Fatalf("writes by a non-leader: %v", h.kube.secretOps)
	}
}

func TestSecretPendingChangesInOneHook(t *testing.T) {
	t.Parallel()
	h := leaderHarness(t)
	h.write("script-update-status", `id=$(secret-add --label first a-key=one)
secret-add --label first a-key=two 2> "$A/dup"
secret-set $id --label renamed --description desc
secret-set $id a-key=two
secret-get $id --refresh --format=json > "$A/refreshed"
secret-info-get --label renamed --format=json > "$A/info"
drop=$(secret-add dropme=1)
secret-remove $drop
echo $id > "$A/id"
`)
	h.tick()
	h.wantHooks("update-status")
	id := strings.TrimSpace(h.read("id"))
	xid := strings.TrimPrefix(id, "secret:")
	if !strings.Contains(h.read("dup"), "already exists") || !strings.Contains(h.read("refreshed"), `"a-key":"two"`) ||
		!strings.Contains(h.read("info"), `"description":"desc"`) || !strings.Contains(h.read("info"), `"label":"renamed"`) {
		t.Fatalf("dup %q refreshed %q info %q", h.read("dup"), h.read("refreshed"), h.read("info"))
	}
	// Created, renamed and updated within one hook it is a single revision; the secret that was dropped never existed.
	if len(h.kube.secrets) != 2 || h.kube.secrets["jk-secret-"+xid+"-2"] != nil {
		t.Fatalf("%v", h.kube.secretOps)
	}
	m := metaFromSecret(h.kube.secrets["jk-secret-"+xid])
	if m.Label != "renamed" || m.Description != "desc" || string(h.kube.secrets["jk-secret-"+xid+"-1"].Data["a-key"]) != "two" {
		t.Fatalf("%+v", m)
	}
}

func TestSecretExpiryAndMetadataUpdate(t *testing.T) {
	t.Parallel()
	h := leaderHarness(t)
	h.write("script-update-status", `secret-add --label e --expire 2h --rotate daily --description d token=abc > "$A/id"`)
	h.tick()
	id := strings.TrimSpace(h.read("id"))
	xid := strings.TrimPrefix(id, "secret:")
	m := metaFromSecret(h.kube.secrets["jk-secret-"+xid])
	want := h.clk.Now().Add(2 * time.Hour).UTC().Truncate(time.Second)
	if m.Expire == nil || !m.Expire.Equal(want) || m.Rotate != "daily" || m.Description != "d" {
		t.Fatalf("%+v want expiry %v", m, want)
	}
	if e := h.kube.appData.OwnedSecrets[xid]; e.ExpireTime == nil || !e.ExpireTime.Time.Equal(want) {
		t.Fatalf("index %+v", e)
	}
	// Metadata only: no new revision.
	h.write("script-update-status", "secret-set "+id+" --expire 5h --description d2 --rotate weekly\nsecret-info-get "+id+" --format=json > \"$A/info\"\n")
	h.tick()
	m = metaFromSecret(h.kube.secrets["jk-secret-"+xid])
	if m.Latest != 1 || m.Description != "d2" || m.Rotate != "weekly" || h.kube.secrets["jk-secret-"+xid+"-2"] != nil || !strings.Contains(h.read("info"), `"rotation":"weekly"`) {
		t.Fatalf("%+v %q", m, h.read("info"))
	}
}

func TestSecretHelpers(t *testing.T) {
	t.Parallel()
	if unitNameOf("my-app-12") != "my-app/12" || unitNameOf("x") != "x" {
		t.Fatal("unitNameOf")
	}
	a, b := settingsVersion(map[string]string{"a": "1", "b": "2"}), settingsVersion(map[string]string{"b": "2", "a": "1"})
	if a != b || a == 0 || settingsVersion(nil) != 0 || settingsVersion(map[string]string{"a": "2"}) == a {
		t.Fatal("settingsVersion")
	}
	if d, changed := seedAddresses(map[string]string{"x": "y"}, "10.0.0.1"); !changed || d["x"] != "y" || d["ingress-address"] != "10.0.0.1" {
		t.Fatalf("%v", d)
	}
	if _, changed := seedAddresses(nil, ""); changed {
		t.Fatal("seeded without an address")
	}
	if got := grantAccess(v1alpha1.SecretGrant{Application: "a", Unit: "a/1", Relation: "3"}); got.Target != "unit-a-1" || got.Scope != "relation-3" {
		t.Fatalf("%+v", got)
	}
	if got := grantAccess(v1alpha1.SecretGrant{Application: "a"}); got.Target != "application-a" || got.Scope != "application-a" {
		t.Fatalf("%+v", got)
	}
	if !equalContent(map[string][]byte{"a": []byte("1")}, map[string][]byte{"a": []byte("1")}) || equalContent(map[string][]byte{"a": []byte("1")}, map[string][]byte{"a": []byte("2")}) || equalContent(nil, map[string][]byte{"a": nil}) {
		t.Fatal("equalContent")
	}
	if !hasGrant([]v1alpha1.SecretGrant{{Application: "a"}}, v1alpha1.SecretGrant{Application: "a", Unit: "a/1"}) || hasGrant(nil, v1alpha1.SecretGrant{Application: "a"}) {
		t.Fatal("hasGrant")
	}
	if !grantIn([]v1alpha1.SecretGrant{{Application: "a"}}, v1alpha1.SecretGrant{Application: "a", Unit: "a/2"}) {
		t.Fatal("grantIn")
	}
	r := newSecretRecorder()
	r.get("x")
	r.get("y")
	r.drop("x")
	if len(r.order) != 1 || r.order[0] != "y" || r.items["x"] != nil {
		t.Fatalf("%+v", r)
	}
	info := interfaceInfo("")
	if info.Address != "" {
		t.Fatal("empty ip")
	}
	if got := interfaceInfo("192.0.2.77"); got.CIDR != "192.0.2.77/32" {
		t.Fatalf("%+v", got)
	}
	var _ hooktools.Backend = (*hookContext)(nil)
}
