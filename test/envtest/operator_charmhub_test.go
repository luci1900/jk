package envtest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/internal/operator"
	"github.com/luci1900/jk/internal/registry"
)

var (
	opHub   *fakeHub
	opStore *fakeStore
)

func testNode(name, arch string) *corev1.Node {
	n := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
	n.Status.NodeInfo.Architecture = arch
	return n
}

// fakeDigest is the digest fakeStore gives a pushed charm.
func fakeDigest(arch string, data []byte) string {
	sum := sha256.Sum256(append([]byte(arch+"\x00"), data...))
	return "sha256:" + hex.EncodeToString(sum[:])
}

type push struct {
	Arch string
	Data []byte
}

// fakeStore is jk-registry's write side.
type fakeStore struct {
	mu     sync.Mutex
	pushes []push
	have   map[string]bool
}

func (f *fakeStore) Exists(_ context.Context, digest string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.have[digest], nil
}

func (f *fakeStore) PushCharm(_ context.Context, r io.ReaderAt, size int64, arch string) (string, error) {
	data := make([]byte, size)
	if _, err := r.ReadAt(data, 0); err != nil && err != io.EOF {
		return "", err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.have == nil {
		f.have = map[string]bool{}
	}
	d := fakeDigest(arch, data)
	f.have[d] = true
	f.pushes = append(f.pushes, push{arch, data})
	return d, nil
}

func (f *fakeStore) pushed() []push {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]push(nil), f.pushes...)
}

type hubCharm struct {
	revision    int
	baseChannel string
	channels    []string
	metadata    string
	config      string
	actions     string
	data        []byte
	// chanRev gives channels their own revision (default: revision); a revision's file is data plus its number.
	chanRev map[string]int
}

func (c *hubCharm) file(rev int) []byte {
	if rev == c.revision {
		return c.data
	}
	return append(append([]byte(nil), c.data...), fmt.Sprintf("-r%d", rev)...)
}

// fakeHub speaks the parts of the Charmhub API the operator uses.
type fakeHub struct {
	srv *httptest.Server

	mu       sync.Mutex
	charms   map[string]*hubCharm
	requests []map[string]any
	failDL   map[string]bool
	corrupt  map[string]bool
}

func newFakeHub() *fakeHub {
	h := &fakeHub{charms: map[string]*hubCharm{}, failDL: map[string]bool{}, corrupt: map[string]bool{}}
	h.srv = httptest.NewServer(h)
	return h
}

func (h *fakeHub) set(name string, c *hubCharm) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if c.channels == nil {
		c.channels = []string{"latest/stable", "14/stable"}
	}
	if c.baseChannel == "" {
		c.baseChannel = "22.04"
	}
	h.charms[name] = c
}

func (h *fakeHub) calls(name string) []map[string]any {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []map[string]any
	for _, r := range h.requests {
		if r["name"] == name {
			out = append(out, r)
		}
	}
	return out
}

func (h *fakeHub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if strings.HasPrefix(r.URL.Path, "/dl/") {
		name := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/dl/"), ".charm")
		name, revStr, _ := strings.Cut(name, "@")
		c := h.charms[name]
		rev := 0
		if c != nil {
			rev = c.revision
		}
		if r, err := strconv.Atoi(revStr); err == nil {
			rev = r
		}
		switch {
		case c == nil:
			http.NotFound(w, r)
		case h.failDL[name]:
			http.Error(w, "boom", http.StatusInternalServerError)
		case h.corrupt[name]:
			_, _ = w.Write([]byte("something else"))
		default:
			_, _ = w.Write(c.file(rev))
		}
		return
	}
	var req struct {
		Actions []map[string]any `json:"actions"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	a := req.Actions[0]
	h.requests = append(h.requests, a)
	name, _ := a["name"].(string)
	base, _ := a["base"].(map[string]any)
	result := func(v map[string]any) {
		_ = json.NewEncoder(w).Encode(map[string]any{"error-list": []any{}, "results": []any{v}})
	}
	apiErr := func(code, msg string, extra map[string]any) {
		e := map[string]any{"code": code, "message": msg}
		if extra != nil {
			e["extra"] = extra
		}
		result(map[string]any{"charm": nil, "error": e})
	}
	c := h.charms[name]
	if c == nil {
		apiErr("name-not-found", "The Charm with the given name was not found in the Store.", nil)
		return
	}
	if base["name"] == "NA" {
		apiErr("invalid-charm-base", "invalid base", map[string]any{"default-bases": []any{map[string]any{"name": "ubuntu", "channel": c.baseChannel, "architecture": base["architecture"]}}})
		return
	}
	channel, _ := a["channel"].(string)
	if a["revision"] == nil {
		found := false
		var releases []any
		for _, ch := range c.channels {
			found = found || ch == channel
			releases = append(releases, map[string]any{"channel": ch, "base": map[string]any{"name": "ubuntu", "channel": c.baseChannel, "architecture": "amd64"}})
		}
		if !found {
			apiErr("revision-not-found", "No revision was found in the Store.", map[string]any{"releases": releases})
			return
		}
	}
	rev := c.revision
	if r, ok := c.chanRev[channel]; ok {
		rev = r
	}
	if r, ok := a["revision"].(float64); ok {
		rev = int(r)
	}
	data := c.file(rev)
	sum := sha256.Sum256(data)
	result(map[string]any{
		"effective-channel": channel,
		"name":              name,
		"result":            "install",
		"charm": map[string]any{
			"id": "id-" + name, "name": name, "revision": rev, "version": fmt.Sprint(rev),
			"bases":         []any{base},
			"download":      map[string]any{"url": fmt.Sprintf("%s/dl/%s@%d.charm", h.srv.URL, name, rev), "hash-sha-256": hex.EncodeToString(sum[:]), "size": len(data)},
			"metadata-yaml": c.metadata, "config-yaml": c.config, "actions-yaml": c.actions,
			"resources": []any{},
		},
	})
}

const pgishMetadata = `name: pgish
assumes: [k8s-api]
containers:
  postgresql:
    resource: postgresql-image
    mounts:
    - storage: pgdata
      location: /var/lib/postgresql/data
resources:
  postgresql-image:
    type: oci-image
    upstream-source: ghcr.io/canonical/charmed-postgresql:14
storage:
  pgdata:
    type: filesystem
    location: /var/lib/postgresql/data
    minimum-size: 2G
peers:
  database-peers: {interface: postgresql_peers}
  restart: {interface: rolling_op}
provides:
  database: {interface: postgresql_client}
requires:
  client: {interface: postgresql_client}
  certificates: {interface: tls-certificates, limit: 1}
`

const pgishConfig = "options:\n  profile:\n    type: string\n    default: production\n"

func addPgish(name string) *hubCharm {
	c := &hubCharm{revision: 959, metadata: strings.Replace(pgishMetadata, "name: pgish", "name: "+name, 1), config: pgishConfig, data: []byte("PK-fake-charm-" + name)}
	opHub.set(name, c)
	return c
}

func newHubApp(t *testing.T, c client.Client, ns, name string, mut func(*v1alpha1.Application)) *v1alpha1.Application {
	t.Helper()
	app := &v1alpha1.Application{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec:       v1alpha1.ApplicationSpec{Charm: v1alpha1.CharmSpec{Name: name}},
	}
	if mut != nil {
		mut(app)
	}
	if err := c.Create(context.Background(), app); err != nil {
		t.Fatal(err)
	}
	return app
}

func readyReason(c client.Client, ns, name string) string {
	a := ready(c, ns, name)
	if a == nil {
		return ""
	}
	cond := meta.FindStatusCondition(a.Status.Conditions, operator.ReadyCondition)
	if cond == nil {
		return ""
	}
	return cond.Reason
}

// updateApp applies fn to the Application and updates it, retrying on conflicts with the operator's own writes.
func updateApp(t *testing.T, c client.Client, ns, name string, fn func(*v1alpha1.Application)) {
	t.Helper()
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		a := ready(c, ns, name)
		if a == nil {
			return fmt.Errorf("application %s/%s not found", ns, name)
		}
		fn(a)
		return c.Update(context.Background(), a)
	})
	if err != nil {
		t.Fatal(err)
	}
}

// touch changes an annotation so the Application is reconciled again.
func touch(t *testing.T, c client.Client, ns, name string) {
	t.Helper()
	updateApp(t, c, ns, name, func(a *v1alpha1.Application) {
		if a.Annotations == nil {
			a.Annotations = map[string]string{}
		}
		a.Annotations["touched"] = fmt.Sprint(len(a.Annotations["touched"]) + 1)
	})
}

func TestCharmhubBareApplication(t *testing.T) {
	// Serial: it counts pushes or registry reads, which the shared fakes keep for the whole package.
	c := startOperator(t)
	ns := newNamespace(t, c, true)
	hc := addPgish("pgish")
	newHubApp(t, c, ns, "pgish", nil) // charm: {name: pgish} and nothing else

	var a *v1alpha1.Application
	eventually(t, "resolved and cached", func() bool {
		a = ready(c, ns, "pgish")
		return a != nil && a.Status.Charm != nil && a.Status.Charm.Image != "" && a.Status.Charm.Metadata != nil
	})
	ch := a.Status.Charm
	digest := fakeDigest("amd64", hc.data)
	sum := sha256.Sum256(hc.data)
	if ch.Revision != 959 || ch.Channel != "latest/stable" || ch.Base != "ubuntu@22.04" || ch.Architecture != "amd64" ||
		ch.Sha256 != hex.EncodeToString(sum[:]) || !strings.HasSuffix(ch.URL, "/dl/pgish@959.charm") ||
		ch.Image != "jk-registry.jk-system.svc:5000/charms@"+digest {
		t.Errorf("status.charm = %+v", ch)
	}
	if ch.ConfigSchema == nil || !strings.Contains(string(ch.ConfigSchema.Raw), `"profile"`) || ch.Actions != nil {
		t.Errorf("config %v actions %v", ch.ConfigSchema, ch.Actions)
	}
	if ch.ResourceImages["postgresql-image"] != "ghcr.io/canonical/charmed-postgresql:14" {
		t.Errorf("resource images %v", ch.ResourceImages)
	}

	// Resolution asked for the cluster's architecture and found the base through the channel's default.
	calls := opHub.calls("pgish")
	if len(calls) != 2 {
		t.Fatalf("hub calls: %v", calls)
	}
	if b := calls[0]["base"].(map[string]any); b["name"] != "NA" || b["architecture"] != "amd64" || calls[0]["channel"] != "latest/stable" {
		t.Errorf("first request %v", calls[0])
	}
	if b := calls[1]["base"].(map[string]any); b["name"] != "ubuntu" || b["channel"] != "22.04" {
		t.Errorf("second request %v", calls[1])
	}
	if p := opStore.pushed(); len(p) != 1 || p[0].Arch != "amd64" || !bytes.Equal(p[0].Data, hc.data) {
		t.Errorf("pushes %+v", p)
	}

	// The pods use the resolved charm, architecture and storage.
	var sts appsv1.StatefulSet
	eventually(t, "statefulset", func() bool { return exists(c, ns, "pgish", &sts) })
	pod := sts.Spec.Template.Spec
	if pod.NodeSelector["kubernetes.io/arch"] != "amd64" || pod.InitContainers[0].Args[len(pod.InitContainers[0].Args)-1] != "--charm-image="+ch.Image {
		t.Errorf("pod spec: %v %v", pod.NodeSelector, pod.InitContainers[0].Args)
	}
	if got := pod.Containers[1].Image; got != "ghcr.io/canonical/charmed-postgresql:14" {
		t.Errorf("workload image %q", got)
	}
	if len(sts.Spec.VolumeClaimTemplates) != 1 || sts.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests.Storage().Cmp(resource.MustParse("2Gi")) != 0 {
		t.Errorf("claims %+v", sts.Spec.VolumeClaimTemplates)
	}
	if sts.Spec.PersistentVolumeClaimRetentionPolicy.WhenScaled != appsv1.DeletePersistentVolumeClaimRetentionPolicyType {
		t.Errorf("retention %+v", sts.Spec.PersistentVolumeClaimRetentionPolicy)
	}

	// One peer Relation per peers: endpoint, each with its own id from the counter.
	ids := map[int64]string{}
	for _, ep := range []string{"database-peers", "restart"} {
		name := v1alpha1.PeerRelationName("pgish", ep)
		var rel v1alpha1.Relation
		eventually(t, "relation "+name+" has an id", func() bool { return get(c, ns, name, &rel) == nil && rel.Status.ID != 0 })
		if prev, dup := ids[rel.Status.ID]; dup {
			t.Errorf("relation id %d used by %s and %s", rel.Status.ID, prev, ep)
		}
		ids[rel.Status.ID] = ep
		e := rel.Spec.Endpoints
		if len(e) != 1 || e[0].Application != "pgish" || e[0].Endpoint != ep || e[0].Namespace != ns || rel.Labels[v1alpha1.PeerEndpointLabel] != ep || !metav1.IsControlledBy(&rel, ready(c, ns, "pgish")) {
			t.Errorf("relation %+v", rel.ObjectMeta)
		}
	}
	var cm corev1.ConfigMap
	_ = get(c, ns, v1alpha1.ModelConfigMap, &cm)
	if cm.Data[operator.RelationIDKey] != "2" {
		t.Errorf("counter %v", cm.Data)
	}

	// Resolution happens once: reconciling again neither asks Charmhub nor pushes again.
	for i := 0; i < 2; i++ {
		touch(t, c, ns, "pgish")
	}
	three := int32(3)
	updateApp(t, c, ns, "pgish", func(a *v1alpha1.Application) { a.Spec.Scale = &three })
	eventually(t, "scale applied", func() bool { return get(c, ns, "pgish", &sts) == nil && *sts.Spec.Replicas == 3 })
	if len(opHub.calls("pgish")) != 2 || len(opStore.pushed()) != 1 {
		t.Errorf("resolved again: %d hub calls, %d pushes", len(opHub.calls("pgish")), len(opStore.pushed()))
	}
	// Peer relations and their ids are stable too.
	for id, ep := range ids {
		var rel v1alpha1.Relation
		_ = get(c, ns, v1alpha1.PeerRelationName("pgish", ep), &rel)
		if rel.Status.ID != id {
			t.Errorf("%s id changed %d -> %d", ep, id, rel.Status.ID)
		}
	}
}

func TestCharmhubPinsAndChannel(t *testing.T) {
	t.Parallel()
	c := startOperator(t)
	ns := newNamespace(t, c, true)
	addPgish("pinned")
	rev := 960
	newHubApp(t, c, ns, "pinned", func(a *v1alpha1.Application) {
		a.Spec.Charm.Channel = "14/stable"
		a.Spec.Charm.Base = "ubuntu@22.04"
		a.Spec.Charm.Revision = &rev
	})
	eventually(t, "resolved", func() bool { a := ready(c, ns, "pinned"); return a.Status.Charm != nil && a.Status.Charm.Image != "" })
	calls := opHub.calls("pinned")
	if len(calls) != 1 || calls[0]["revision"] != float64(960) || calls[0]["channel"] != nil {
		t.Errorf("a revision pin must be one request without a channel: %v", calls)
	}
	if b := calls[0]["base"].(map[string]any); b["name"] != "ubuntu" || b["channel"] != "22.04" {
		t.Errorf("base %v", b)
	}

	// A channel the charm isn't released in: reported with what is available, not retried quickly.
	addPgish("nochan")
	newHubApp(t, c, ns, "nochan", func(a *v1alpha1.Application) { a.Spec.Charm.Channel = "9/stable" })
	eventually(t, "CharmNotFound", func() bool { return readyReason(c, ns, "nochan") == "CharmNotFound" })
	cond := meta.FindStatusCondition(ready(c, ns, "nochan").Status.Conditions, operator.ReadyCondition)
	if !strings.Contains(cond.Message, "9/stable") || !strings.Contains(cond.Message, "14/stable (ubuntu@22.04 amd64)") {
		t.Errorf("message %q", cond.Message)
	}
	if !gone(c, ns, "nochan", &appsv1.StatefulSet{}) {
		t.Error("statefulset for an unresolved charm")
	}
	// Fixing the spec resolves it.
	updateApp(t, c, ns, "nochan", func(a *v1alpha1.Application) { a.Spec.Charm.Channel = "14/stable" })
	eventually(t, "resolved after fixing the channel", func() bool {
		a := ready(c, ns, "nochan")
		return exists(c, ns, "nochan", &appsv1.StatefulSet{}) && a != nil && a.Status.Charm != nil && a.Status.Charm.Channel != ""
	})
	if got := ready(c, ns, "nochan").Status.Charm.Channel; got != "14/stable" {
		t.Errorf("channel %q", got)
	}

	// A charm that doesn't exist.
	newHubApp(t, c, ns, "nosuch", nil)
	eventually(t, "name not found", func() bool { return readyReason(c, ns, "nosuch") == "CharmNotFound" })
}

func TestCharmhubDownloadFailuresAreRetriedWithoutResolvingAgain(t *testing.T) {
	// Serial: it counts pushes or registry reads, which the shared fakes keep for the whole package.
	c := startOperator(t)
	ns := newNamespace(t, c, true)
	addPgish("flaky")
	opHub.mu.Lock()
	opHub.failDL["flaky"] = true
	opHub.mu.Unlock()
	newHubApp(t, c, ns, "flaky", nil)
	eventually(t, "CharmUnavailable", func() bool { return readyReason(c, ns, "flaky") == "CharmUnavailable" })
	ch := ready(c, ns, "flaky").Status.Charm
	if ch == nil || ch.URL == "" || ch.Sha256 == "" || ch.Image != "" || ch.Revision != 959 {
		t.Fatalf("a failed download keeps the resolution: %+v", ch)
	}
	if !gone(c, ns, "flaky", &appsv1.StatefulSet{}) {
		t.Error("statefulset without a charm image")
	}
	before := len(opHub.calls("flaky"))

	// Corrupt bytes are rejected by the checksum and nothing is pushed.
	opHub.mu.Lock()
	opHub.failDL["flaky"] = false
	opHub.corrupt["flaky"] = true
	opHub.mu.Unlock()
	pushes := len(opStore.pushed())
	touch(t, c, ns, "flaky")
	eventually(t, "checksum error", func() bool {
		cond := meta.FindStatusCondition(ready(c, ns, "flaky").Status.Conditions, operator.ReadyCondition)
		return cond != nil && strings.Contains(cond.Message, "checksum mismatch")
	})
	if len(opStore.pushed()) != pushes {
		t.Error("a corrupt charm was pushed")
	}

	opHub.mu.Lock()
	opHub.corrupt["flaky"] = false
	opHub.mu.Unlock()
	touch(t, c, ns, "flaky")
	eventually(t, "recovered", func() bool {
		return exists(c, ns, "flaky", &appsv1.StatefulSet{}) && ready(c, ns, "flaky").Status.Charm.Image != ""
	})
	if n := len(opHub.calls("flaky")); n != before {
		t.Errorf("resolved again after a download failure: %d -> %d store requests", before, n)
	}
}

func TestCharmhubURLPinnedSkipsTheStore(t *testing.T) {
	t.Parallel()
	c := startOperator(t)
	ns := newNamespace(t, c, true)
	data := []byte("PK-pinned-bytes")
	opHub.set("urlpin", &hubCharm{revision: 7, data: data})
	sum := sha256.Sum256(data)
	digest := fakeDigest("amd64", data)
	opCharms.add(digest, &registry.Charm{
		Metadata: map[string]any{"name": "urlpin", "containers": map[string]any{"w": map[string]any{"resource": "img"}}, "resources": map[string]any{"img": map[string]any{"type": "oci-image", "upstream-source": "busybox:1"}}},
		Base:     registry.Base{Name: "ubuntu", Channel: "24.04"},
	})
	rev := 7
	newHubApp(t, c, ns, "urlpin", func(a *v1alpha1.Application) {
		a.Spec.Charm.URL = opHub.srv.URL + "/dl/urlpin.charm"
		a.Spec.Charm.Sha256 = hex.EncodeToString(sum[:])
		a.Spec.Charm.Revision = &rev
	})
	eventually(t, "statefulset", func() bool { return exists(c, ns, "urlpin", &appsv1.StatefulSet{}) })
	ch := ready(c, ns, "urlpin").Status.Charm
	if ch.Revision != 7 || ch.Base != "ubuntu@24.04" || ch.Image != "jk-registry.jk-system.svc:5000/charms@"+digest || ch.Metadata == nil {
		t.Errorf("status.charm %+v", ch)
	}
	if n := len(opHub.calls("urlpin")); n != 0 {
		t.Errorf("fully pinned charms need no store lookup, got %d requests", n)
	}

	// url without sha256 is refused.
	newHubApp(t, c, ns, "halfpin", func(a *v1alpha1.Application) { a.Spec.Charm.URL = opHub.srv.URL + "/dl/x.charm" })
	eventually(t, "invalid", func() bool { return readyReason(c, ns, "halfpin") == "InvalidApplication" })

	// A URL outside Charmhub is never fetched.
	newHubApp(t, c, ns, "evil", func(a *v1alpha1.Application) {
		a.Spec.Charm.URL = "http://169.254.169.254/latest/meta-data"
		a.Spec.Charm.Sha256 = strings.Repeat("0", 64)
	})
	eventually(t, "refused", func() bool {
		cond := meta.FindStatusCondition(ready(c, ns, "evil").Status.Conditions, operator.ReadyCondition)
		return cond != nil && strings.Contains(cond.Message, "not a Charmhub host")
	})
}

func TestMixedArchitectureNeedsConstraint(t *testing.T) {
	c := startOperator(t)
	ctx := context.Background()
	extra := testNode("node-arm64", "arm64")
	if err := c.Create(ctx, extra); err != nil {
		t.Fatal(err)
	}
	removed := false
	removeNode := func() {
		if !removed {
			removed = true
			_ = c.Delete(ctx, extra)
		}
	}
	t.Cleanup(removeNode)
	ns := newNamespace(t, c, true)
	addPgish("mixed")
	newHubApp(t, c, ns, "mixed", nil)
	eventually(t, "mixed architectures rejected", func() bool {
		a := ready(c, ns, "mixed")
		cond := meta.FindStatusCondition(a.Status.Conditions, operator.ReadyCondition)
		return cond != nil && cond.Reason == "InvalidApplication" && strings.Contains(cond.Message, "amd64 and arm64") && strings.Contains(cond.Message, "constraints.arch")
	})
	if n := len(opHub.calls("mixed")); n != 0 {
		t.Errorf("asked Charmhub without an architecture: %d", n)
	}

	// The constraint decides, for the charm and the node selector.
	addPgish("mixedarm")
	newHubApp(t, c, ns, "mixedarm", func(a *v1alpha1.Application) { a.Spec.Constraints = &v1alpha1.Constraints{Arch: "arm64"} })
	var sts appsv1.StatefulSet
	eventually(t, "constrained app deployed", func() bool { return exists(c, ns, "mixedarm", &sts) && ready(c, ns, "mixedarm").Status.Charm != nil })
	if sts.Spec.Template.Spec.NodeSelector["kubernetes.io/arch"] != "arm64" || ready(c, ns, "mixedarm").Status.Charm.Architecture != "arm64" {
		t.Errorf("selector %v", sts.Spec.Template.Spec.NodeSelector)
	}
	if calls := opHub.calls("mixedarm"); calls[0]["base"].(map[string]any)["architecture"] != "arm64" {
		t.Errorf("request %v", calls[0])
	}

	// When the other architecture goes away the unconstrained app resolves by itself (retried every few seconds).
	removeNode()
	eventually(t, "unconstrained app resolves once the cluster is uniform", func() bool {
		return exists(c, ns, "mixed", &appsv1.StatefulSet{}) && ready(c, ns, "mixed").Status.Charm != nil
	})
	if ready(c, ns, "mixed").Status.Charm.Architecture != "amd64" {
		t.Errorf("arch %q", ready(c, ns, "mixed").Status.Charm.Architecture)
	}
}

func TestPeerRelationIDsAreUniqueAndNeverReused(t *testing.T) {
	t.Parallel()
	c := startOperator(t)
	ctx := context.Background()
	ns := newNamespace(t, c, true)
	addPgish("ida")
	addPgish("idb")
	newHubApp(t, c, ns, "ida", nil)
	newHubApp(t, c, ns, "idb", nil)
	idOf := func(app, ep string) int64 {
		var rel v1alpha1.Relation
		if get(c, ns, v1alpha1.PeerRelationName(app, ep), &rel) != nil {
			return 0
		}
		return rel.Status.ID
	}
	var all []int64
	eventually(t, "four relations with ids", func() bool {
		all = nil
		for _, app := range []string{"ida", "idb"} {
			for _, ep := range []string{"database-peers", "restart"} {
				all = append(all, idOf(app, ep))
			}
		}
		for _, id := range all {
			if id == 0 {
				return false
			}
		}
		return true
	})
	seen := map[int64]bool{}
	for _, id := range all {
		if seen[id] || id < 1 || id > 4 {
			t.Fatalf("ids %v: duplicate or out of range", all)
		}
		seen[id] = true
	}

	// A user-created Relation gets an id from the same counter, and ids of deleted Relations are not reused.
	if err := c.Delete(ctx, &v1alpha1.Relation{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: v1alpha1.PeerRelationName("ida", "restart")}}); err != nil {
		t.Fatal(err)
	}
	extra := &v1alpha1.Relation{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "extra"},
		Spec: v1alpha1.RelationSpec{Endpoints: []v1alpha1.EndpointRef{
			{Namespace: ns, Application: "ida", Endpoint: "database"}, {Namespace: ns, Application: "idb", Endpoint: "client"}}},
	}
	if err := c.Create(ctx, extra); err != nil {
		t.Fatal(err)
	}
	eventually(t, "extra relation id", func() bool { return get(c, ns, "extra", extra) == nil && extra.Status.ID != 0 })
	if seen[extra.Status.ID] {
		t.Errorf("id %d reused", extra.Status.ID)
	}
	// The Application controller re-creates the deleted peer relation with a new id.
	eventually(t, "peer relation re-created with a new id", func() bool {
		id := idOf("ida", "restart")
		return id != 0 && !seen[id] && id != extra.Status.ID
	})

	// Not a jk model: no id.
	other := newNamespace(t, c, false)
	rel := &v1alpha1.Relation{ObjectMeta: metav1.ObjectMeta{Namespace: other, Name: "r"}, Spec: v1alpha1.RelationSpec{Endpoints: []v1alpha1.EndpointRef{{Namespace: other, Application: "a", Endpoint: "e"}}}}
	if err := c.Create(ctx, rel); err != nil {
		t.Fatal(err)
	}
	consistently(t, "relation in an unlabelled namespace got an id", func() bool {
		return get(c, other, "r", rel) == nil && rel.Status.ID == 0
	})
}

func unitData(t *testing.T, c client.Client, ns string, app *v1alpha1.Application, n int, ports ...v1alpha1.PortRange) *v1alpha1.UnitData {
	t.Helper()
	u := &v1alpha1.UnitData{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: fmt.Sprintf("%s-%d", app.Name, n), OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(app, v1alpha1.GroupVersion.WithKind("Application"))}},
		Spec:       v1alpha1.UnitDataSpec{OpenedPorts: ports},
	}
	if err := c.Create(context.Background(), u); err != nil {
		t.Fatal(err)
	}
	return u
}

func servicePorts(c client.Client, ns, name string) string {
	var svc corev1.Service
	if get(c, ns, name, &svc) != nil {
		return "?"
	}
	var out []string
	for _, p := range svc.Spec.Ports {
		out = append(out, fmt.Sprintf("%s:%d>%d/%s", p.Name, p.Port, p.TargetPort.IntVal, p.Protocol))
	}
	return strings.Join(out, ",")
}

func TestServicePortsFromOpenedPorts(t *testing.T) {
	t.Parallel()
	c := startOperator(t)
	ctx := context.Background()
	ns := newNamespace(t, c, true)
	newApp(t, c, ns, "simple", digestSimple, 2)
	eventually(t, "placeholder port", func() bool { return servicePorts(c, ns, "simple") == "placeholder:65535>65535/TCP" })
	app := ready(c, ns, "simple")

	u0 := unitData(t, c, ns, app, 0, v1alpha1.PortRange{Protocol: "tcp", From: 5432, To: 5432})
	unitData(t, c, ns, app, 1, v1alpha1.PortRange{Protocol: "tcp", From: 5432, To: 5432}, v1alpha1.PortRange{Protocol: "udp", From: 53, To: 53})
	eventually(t, "union of the units' ports", func() bool {
		return servicePorts(c, ns, "simple") == "juju-53-udp:53>53/UDP,juju-5432-tcp:5432>5432/TCP"
	})
	// A unit closing a port the other still has open changes nothing; closing the last one removes it.
	u0.Spec.OpenedPorts = nil
	if err := c.Update(ctx, u0); err != nil {
		t.Fatal(err)
	}
	var u1 v1alpha1.UnitData
	_ = get(c, ns, "simple-1", &u1)
	u1.Spec.OpenedPorts = []v1alpha1.PortRange{{Protocol: "tcp", From: 5432, To: 5432}}
	if err := c.Update(ctx, &u1); err != nil {
		t.Fatal(err)
	}
	eventually(t, "udp port closed", func() bool { return servicePorts(c, ns, "simple") == "juju-5432-tcp:5432>5432/TCP" })

	// Units above the scale no longer count.
	one := int32(1)
	updateApp(t, c, ns, "simple", func(a *v1alpha1.Application) { a.Spec.Scale = &one })
	eventually(t, "back to the placeholder", func() bool { return servicePorts(c, ns, "simple") == "placeholder:65535>65535/TCP" })
}

func TestStatefulSetFieldsPatchedByTheCharmSurvive(t *testing.T) {
	t.Parallel()
	c := startOperator(t)
	ctx := context.Background()
	ns := newNamespace(t, c, true)
	newApp(t, c, ns, "simple", digestSimple, 1)
	var sts appsv1.StatefulSet
	eventually(t, "statefulset", func() bool { return exists(c, ns, "simple", &sts) })

	// postgresql-k8s patches the rollout partition (and charms add other things) as their own field manager.
	patch := []byte(`{"spec":{"updateStrategy":{"type":"RollingUpdate","rollingUpdate":{"partition":2}},"template":{"metadata":{"annotations":{"charm":"was-here"}}}}}`)
	if err := c.Patch(ctx, &sts, client.RawPatch(types.MergePatchType, patch), client.FieldOwner("charm")); err != nil {
		t.Fatal(err)
	}
	// The operator reconciles again with a changed spec (scale) and must neither revert nor claim the fields.
	three := int32(3)
	updateApp(t, c, ns, "simple", func(a *v1alpha1.Application) { a.Spec.Scale = &three })
	eventually(t, "scale applied", func() bool { return get(c, ns, "simple", &sts) == nil && *sts.Spec.Replicas == 3 })
	touch(t, c, ns, "simple")
	consistently(t, "the charm's partition or annotation was reverted", func() bool {
		if get(c, ns, "simple", &sts) != nil {
			return false
		}
		ru := sts.Spec.UpdateStrategy.RollingUpdate
		return ru != nil && ru.Partition != nil && *ru.Partition == 2 && sts.Spec.Template.Annotations["charm"] == "was-here"
	})
	for _, mf := range sts.ManagedFields {
		if mf.Manager == operator.FieldOwner && mf.FieldsV1 != nil && strings.Contains(string(mf.FieldsV1.Raw), "updateStrategy") {
			t.Errorf("the operator owns updateStrategy: %s", mf.FieldsV1.Raw)
		}
	}
}

func TestVolumesAreRetainedAndDestroyedOnRequest(t *testing.T) {
	t.Parallel()
	c := startOperator(t)
	ctx := context.Background()
	ns := newNamespace(t, c, true)
	addPgish("vols")
	newHubApp(t, c, ns, "vols", func(a *v1alpha1.Application) {
		a.Spec.Storage = map[string]v1alpha1.StorageSpec{"pgdata": {Size: "5G"}}
	})
	var sts appsv1.StatefulSet
	eventually(t, "statefulset", func() bool { return exists(c, ns, "vols", &sts) })
	if q := sts.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests.Storage(); q.Cmp(resource.MustParse("5Gi")) != 0 {
		t.Errorf("spec.storage size ignored: %v", q)
	}

	// The StatefulSet controller and a provisioner don't run here: play them. Each unit gets a bound volume.
	mkVolume := func(pvName, pvcName string) {
		pv := &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: pvName},
			Spec: corev1.PersistentVolumeSpec{
				Capacity:                      corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("5Gi")},
				AccessModes:                   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
				PersistentVolumeSource:        corev1.PersistentVolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/tmp/" + pvName}},
			},
		}
		if err := c.Create(ctx, pv); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Delete(ctx, pv) })
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: pvcName, Labels: operator.PodLabels("vols")},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, VolumeName: pvName,
				Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("5Gi")}},
			},
		}
		if err := c.Create(ctx, pvc); err != nil {
			t.Fatal(err)
		}
		pvc.Status.Phase = corev1.ClaimBound
		if err := c.Status().Update(ctx, pvc); err != nil {
			t.Fatal(err)
		}
	}
	pvName := func(n string) string { return ns + "-" + n }
	mkVolume(pvName("pv0"), "vols-pgdata-1a2b3c4d-vols-0")
	mkVolume(pvName("pv1"), "vols-pgdata-1a2b3c4d-vols-1")
	policy := func(name string) corev1.PersistentVolumeReclaimPolicy {
		var pv corev1.PersistentVolume
		if c.Get(ctx, client.ObjectKey{Name: name}, &pv) != nil {
			return ""
		}
		return pv.Spec.PersistentVolumeReclaimPolicy
	}
	for _, n := range []string{"pv0", "pv1"} {
		eventually(t, n+" retained", func() bool { return policy(pvName(n)) == corev1.PersistentVolumeReclaimRetain })
		var pv corev1.PersistentVolume
		_ = c.Get(ctx, client.ObjectKey{Name: pvName(n)}, &pv)
		if pv.Labels[v1alpha1.AppLabel] != "vols" || pv.Labels[v1alpha1.NamespaceLabel] != ns {
			t.Errorf("%s labels %v", n, pv.Labels)
		}
	}
	// A claim of another app in the namespace is left alone.
	mkVolume(pvName("other"), "something-else")
	consistently(t, "an unrelated volume was touched", func() bool { return policy(pvName("other")) == corev1.PersistentVolumeReclaimDelete })

	// Unit 1 goes away (scale down): its PVC is deleted by the StatefulSet controller, the PV stays, Released.
	// A volume Released while the operator was not watching is still found by its claim name.
	if err := c.Delete(ctx, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "vols-pgdata-1a2b3c4d-vols-1"}}); err != nil {
		t.Fatal(err)
	}
	consistently(t, "the released volume's policy changed", func() bool { return policy(pvName("pv1")) == corev1.PersistentVolumeReclaimRetain })

	// Removing the application without the annotation keeps every volume.
	if err := c.Delete(ctx, ready(c, ns, "vols")); err != nil {
		t.Fatal(err)
	}
	eventually(t, "application removed", func() bool { return ready(c, ns, "vols") == nil })
	if policy(pvName("pv0")) != corev1.PersistentVolumeReclaimRetain || policy(pvName("pv1")) != corev1.PersistentVolumeReclaimRetain {
		t.Error("volumes were not kept")
	}

	// With the annotation, the finalizer sets the app's volumes to Delete (bound and released ones), and only those.
	addPgish("vols2")
	newHubApp(t, c, ns, "vols2", func(a *v1alpha1.Application) {
		a.Annotations = map[string]string{v1alpha1.DestroyStorageAnnotation: "true"}
	})
	eventually(t, "vols2 statefulset", func() bool { return exists(c, ns, "vols2", &appsv1.StatefulSet{}) })
	for _, n := range []string{"a", "b"} {
		pvc := fmt.Sprintf("vols2-pgdata-1a2b3c4d-vols2-%d", map[string]int{"a": 0, "b": 1}[n])
		pv := &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: pvName("d" + n)},
			Spec: corev1.PersistentVolumeSpec{
				Capacity:                      corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
				AccessModes:                   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
				PersistentVolumeSource:        corev1.PersistentVolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/tmp/d" + n}},
				ClaimRef:                      &corev1.ObjectReference{Namespace: ns, Name: pvc},
			},
		}
		if err := c.Create(ctx, pv); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Delete(ctx, pv) })
	}
	if err := c.Delete(ctx, ready(c, ns, "vols2")); err != nil {
		t.Fatal(err)
	}
	eventually(t, "vols2 removed", func() bool { return ready(c, ns, "vols2") == nil })
	for _, n := range []string{"da", "db"} {
		if policy(pvName(n)) != corev1.PersistentVolumeReclaimDelete {
			t.Errorf("%s not marked for deletion: %q", n, policy(pvName(n)))
		}
	}
	if policy(pvName("pv0")) != corev1.PersistentVolumeReclaimRetain {
		t.Error("another app's volume was marked for deletion")
	}
}

func roleHasFullAccess(r *rbacv1.Role) bool {
	for _, rule := range r.Rules {
		if len(rule.Resources) == 1 && rule.Resources[0] == "*" && len(rule.Verbs) == 1 && rule.Verbs[0] == "*" {
			return true
		}
	}
	return false
}

func TestTrustRBAC(t *testing.T) {
	t.Parallel()
	c := startOperator(t)
	ctx := context.Background()
	ns := newNamespace(t, c, true)
	app := newApp(t, c, ns, "trusty", digestSimple, 1)
	var role rbacv1.Role
	eventually(t, "role", func() bool { return exists(c, ns, "trusty", &role) })
	if roleHasFullAccess(&role) {
		t.Error("no trust, full access")
	}
	var execRule bool
	for _, r := range role.Rules {
		for _, res := range r.Resources {
			execRule = execRule || res == "pods/exec"
		}
	}
	if !execRule {
		t.Errorf("juju's default namespace rules missing: %+v", role.Rules)
	}
	cname := operator.ClusterObjectName(ns, "trusty")
	if !gone(c, "", cname, &rbacv1.ClusterRole{}) {
		t.Error("cluster role without trust")
	}

	setTrust := func(tr v1alpha1.Trust) {
		updateApp(t, c, ns, "trusty", func(a *v1alpha1.Application) { a.Spec.Trust = tr })
	}
	setTrust(v1alpha1.TrustNamespace)
	eventually(t, "namespace trust", func() bool { return get(c, ns, "trusty", &role) == nil && roleHasFullAccess(&role) })
	if !gone(c, "", cname, &rbacv1.ClusterRole{}) {
		t.Error("cluster role with namespace trust")
	}

	setTrust(v1alpha1.TrustCluster)
	var cr rbacv1.ClusterRole
	var crb rbacv1.ClusterRoleBinding
	eventually(t, "cluster trust", func() bool { return get(c, "", cname, &cr) == nil && get(c, "", cname, &crb) == nil })
	if cr.Labels[v1alpha1.AppLabel] != "trusty" || cr.Labels[v1alpha1.NamespaceLabel] != ns || crb.RoleRef.Name != cname ||
		len(crb.Subjects) != 1 || crb.Subjects[0].Name != "trusty" || crb.Subjects[0].Namespace != ns || len(cr.Rules) != 1 || cr.Rules[0].Verbs[0] != "*" {
		t.Errorf("cluster objects: %+v %+v", cr, crb)
	}
	if !roleHasFullAccess(&role) {
		eventually(t, "role keeps full access", func() bool { return get(c, ns, "trusty", &role) == nil && roleHasFullAccess(&role) })
	}

	// Lowering the trust removes the cluster objects; raising it again and removing the app removes them too.
	setTrust(v1alpha1.TrustNone)
	eventually(t, "cluster objects removed", func() bool {
		return gone(c, "", cname, &rbacv1.ClusterRole{}) && gone(c, "", cname, &rbacv1.ClusterRoleBinding{})
	})
	eventually(t, "role narrowed", func() bool { return get(c, ns, "trusty", &role) == nil && !roleHasFullAccess(&role) })
	setTrust(v1alpha1.TrustCluster)
	eventually(t, "cluster objects back", func() bool { return exists(c, "", cname, &rbacv1.ClusterRole{}) })
	if err := c.Delete(ctx, app); err != nil {
		t.Fatal(err)
	}
	eventually(t, "application removed", func() bool { return ready(c, ns, "trusty") == nil })
	if !gone(c, "", cname, &rbacv1.ClusterRole{}) || !gone(c, "", cname, &rbacv1.ClusterRoleBinding{}) {
		t.Error("the finalizer left cluster-scoped objects behind")
	}
}

func TestClusterObjectsOfOtherAppsSurvive(t *testing.T) {
	t.Parallel()
	c := startOperator(t)
	ctx := context.Background()
	nsA, nsB := newNamespace(t, c, true), newNamespace(t, c, true)
	for _, ns := range []string{nsA, nsB} {
		newApp(t, c, ns, "same", digestSimple, 1)
		updateApp(t, c, ns, "same", func(a *v1alpha1.Application) { a.Spec.Trust = v1alpha1.TrustCluster })
	}
	for _, ns := range []string{nsA, nsB} {
		n := operator.ClusterObjectName(ns, "same")
		eventually(t, "cluster role "+n, func() bool { return exists(c, "", n, &rbacv1.ClusterRole{}) })
	}
	if err := c.Delete(ctx, ready(c, nsA, "same")); err != nil {
		t.Fatal(err)
	}
	eventually(t, "A removed", func() bool { return ready(c, nsA, "same") == nil })
	if !gone(c, "", operator.ClusterObjectName(nsA, "same"), &rbacv1.ClusterRole{}) {
		t.Error("A's cluster role left")
	}
	if !exists(c, "", operator.ClusterObjectName(nsB, "same"), &rbacv1.ClusterRole{}) {
		t.Error("B's cluster role was deleted along with A's")
	}
}

func secretObj(ns, name, xid, app string, grants []v1alpha1.SecretGrant) *corev1.Secret {
	s := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, Labels: map[string]string{v1alpha1.SecretLabel: xid, v1alpha1.AppLabel: app}}}
	if grants != nil {
		b, _ := json.Marshal(grants)
		s.Annotations = map[string]string{v1alpha1.SecretGrantsAnnotation: string(b)}
	}
	return s
}

func secretsRole(c client.Client, ns, app string) map[string]string {
	var role rbacv1.Role
	if get(c, ns, operator.SecretsRoleName(app), &role) != nil {
		return nil
	}
	out := map[string]string{}
	for _, r := range role.Rules {
		for _, n := range r.ResourceNames {
			out[n] = strings.Join(r.Verbs, ",")
		}
	}
	return out
}

func TestSecretsRoleFollowsOwnershipAndGrants(t *testing.T) {
	t.Parallel()
	c := startOperator(t)
	ctx := context.Background()
	ns := newNamespace(t, c, true)
	newApp(t, c, ns, "owner", digestSimple, 1)
	newApp(t, c, ns, "reader", digestSimple, 1)
	eventually(t, "empty secrets roles", func() bool {
		return secretsRole(c, ns, "owner") != nil && len(secretsRole(c, ns, "owner")) == 0 && secretsRole(c, ns, "reader") != nil
	})
	var rb rbacv1.RoleBinding
	if err := get(c, ns, operator.SecretsRoleName("owner"), &rb); err != nil || rb.Subjects[0].Name != "owner" || rb.RoleRef.Name != operator.SecretsRoleName("owner") {
		t.Errorf("binding %v %+v", err, rb)
	}

	// An app-owned secret shared with its own app (grants to the same app change nothing) and with another app.
	meta1 := secretObj(ns, "jk-secret-abc", "abc", "owner", []v1alpha1.SecretGrant{{Application: "owner"}})
	rev1 := secretObj(ns, "jk-secret-abc-1", "abc", "owner", nil)
	for _, s := range []*corev1.Secret{meta1, rev1} {
		if err := c.Create(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	unlabelled := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "plain"}}
	if err := c.Create(ctx, unlabelled); err != nil {
		t.Fatal(err)
	}
	eventually(t, "owner can manage its secrets", func() bool {
		r := secretsRole(c, ns, "owner")
		return strings.Contains(r["jk-secret-abc"], "update") && strings.Contains(r["jk-secret-abc-1"], "delete")
	})
	if r := secretsRole(c, ns, "reader"); len(r) != 0 {
		t.Errorf("reader sees %v", r)
	}
	if _, ok := secretsRole(c, ns, "owner")["plain"]; ok {
		t.Error("unlabelled secret in the role")
	}

	// A new revision shows up; a grant to another application gives it read access only.
	if err := c.Create(ctx, secretObj(ns, "jk-secret-abc-2", "abc", "owner", nil)); err != nil {
		t.Fatal(err)
	}
	var m corev1.Secret
	_ = get(c, ns, "jk-secret-abc", &m)
	m.Annotations = map[string]string{v1alpha1.SecretGrantsAnnotation: `[{"application":"owner"},{"application":"reader","unit":"reader/0"}]`}
	if err := c.Update(ctx, &m); err != nil {
		t.Fatal(err)
	}
	eventually(t, "reader granted all revisions, read-only", func() bool {
		r := secretsRole(c, ns, "reader")
		return r["jk-secret-abc"] == "get,list,watch" && r["jk-secret-abc-1"] == "get,list,watch" && r["jk-secret-abc-2"] == "get,list,watch"
	})
	if !strings.Contains(secretsRole(c, ns, "owner")["jk-secret-abc-2"], "update") {
		t.Error("owner lost write access to the new revision")
	}

	// Revoking the grant and removing a revision shrinks the roles.
	m.Annotations = map[string]string{v1alpha1.SecretGrantsAnnotation: `[]`}
	if err := c.Update(ctx, &m); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ctx, rev1); err != nil {
		t.Fatal(err)
	}
	eventually(t, "reader revoked, revision dropped", func() bool {
		_, still := secretsRole(c, ns, "owner")["jk-secret-abc-1"]
		return len(secretsRole(c, ns, "reader")) == 0 && !still
	})

	// The agent Role lets any unit create secrets, but not read them by itself.
	var agent rbacv1.Role
	if err := get(c, ns, "owner", &agent); err != nil {
		t.Fatal(err)
	}
	var create, read bool
	for _, r := range agent.Rules {
		for _, res := range r.Resources {
			for _, v := range r.Verbs {
				create = create || (res == "secrets" && v == "create")
				read = read || (res == "secrets" && (v == "get" || v == "list"))
			}
		}
	}
	if !create || read {
		t.Errorf("agent role secrets: create=%v read=%v", create, read)
	}
}
