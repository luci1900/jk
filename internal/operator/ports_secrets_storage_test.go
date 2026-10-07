package operator

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/pkg/charmhub"
)

func pr(proto string, from, to int32) v1alpha1.PortRange {
	return v1alpha1.PortRange{Protocol: proto, From: from, To: to}
}

func TestUnionPortsAndServicePorts(t *testing.T) {
	union := UnionPorts(
		[]v1alpha1.PortRange{pr("tcp", 8008, 8008), pr("TCP", 5432, 5432)},
		[]v1alpha1.PortRange{pr("tcp", 5432, 5432), pr("udp", 5432, 5432), pr("tcp", 9000, 9010)},
		nil,
	)
	if len(union) != 4 || union[0] != pr("tcp", 5432, 5432) || union[1] != pr("udp", 5432, 5432) || union[2] != pr("tcp", 8008, 8008) || union[3] != pr("tcp", 9000, 9010) {
		t.Fatalf("union %+v", union)
	}
	got := ServicePorts(append(union, pr("icmp", 0, 0), pr("tcp", 0, 5), pr("tcp", 70000, 70000), pr("sctp", 38412, 38412), pr("tcp", 20, 10)))
	want := []corev1.ServicePort{
		{Name: "juju-5432-tcp", Port: 5432, TargetPort: intstr.FromInt32(5432), Protocol: corev1.ProtocolTCP},
		{Name: "juju-5432-udp", Port: 5432, TargetPort: intstr.FromInt32(5432), Protocol: corev1.ProtocolUDP},
		{Name: "juju-8008-tcp", Port: 8008, TargetPort: intstr.FromInt32(8008), Protocol: corev1.ProtocolTCP},
		{Name: "juju-9000-9010-tcp", Port: 9000, TargetPort: intstr.FromInt32(9010), Protocol: corev1.ProtocolTCP},
		{Name: "juju-38412-sctp", Port: 38412, TargetPort: intstr.FromInt32(38412), Protocol: corev1.ProtocolSCTP},
	}
	if len(got) != len(want) {
		t.Fatalf("ports %+v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("port %d = %+v, want %+v", i, got[i], want[i])
		}
	}
	for _, none := range [][]v1alpha1.PortRange{nil, {pr("icmp", 0, 0)}} {
		p := ServicePorts(none)
		if len(p) != 1 || p[0].Name != "placeholder" || p[0].Port != 65535 {
			t.Errorf("placeholder: %+v", p)
		}
	}
}

func TestOpenedPortsOnlyCurrentUnits(t *testing.T) {
	app := testApp(pgMetadata) // scale 2
	unit := func(name string, ports ...v1alpha1.PortRange) v1alpha1.UnitData {
		return v1alpha1.UnitData{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: v1alpha1.UnitDataSpec{OpenedPorts: ports}}
	}
	foreign := unit("pg-1", pr("tcp", 1111, 1111))
	foreign.OwnerReferences = []metav1.OwnerReference{{UID: "someone-else"}}
	got := openedPorts(app, []v1alpha1.UnitData{
		unit("pg-0", pr("tcp", 5432, 5432)), unit("pg-1", pr("tcp", 8008, 8008)), unit("pg-2", pr("tcp", 9999, 9999)),
		unit("other-0", pr("tcp", 1, 1)), unit("pg-x", pr("tcp", 2, 2)), foreign,
	})
	if len(got) != 2 || got[0].From != 5432 || got[1].From != 8008 {
		t.Errorf("ports %+v", got)
	}
}

func sec(name, xid, app string, grants string) corev1.Secret {
	s := corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{v1alpha1.AppLabel: app}}}
	if xid != "" {
		s.Labels[v1alpha1.SecretLabel] = xid
	}
	if grants != "" {
		s.Annotations = map[string]string{v1alpha1.SecretGrantsAnnotation: grants}
	}
	return s
}

func TestSecretNamesAndRules(t *testing.T) {
	secrets := []corev1.Secret{
		sec("jk-secret-a", "a", "pg", `[{"application":"pg"},{"application":"web","unit":"web/0"}]`),
		sec("jk-secret-a-1", "a", "pg", ""),
		sec("jk-secret-a-2", "a", "pg", ""),
		sec("jk-secret-b", "b", "web", `[{"application":"pg"}]`),
		sec("jk-secret-b-1", "b", "web", ""),
		sec("jk-secret-c", "c", "web", `not json`),
		sec("jk-secret-c-1", "c", "web", ""),
		sec("jk-secret-d", "d", "web", `[{"application":""}]`),
		sec("unrelated", "", "pg", ""),
	}
	owned, granted := SecretNames("pg", secrets, nil)
	if strings.Join(owned, ",") != "jk-secret-a,jk-secret-a-1,jk-secret-a-2" || strings.Join(granted, ",") != "jk-secret-b,jk-secret-b-1" {
		t.Errorf("pg owned=%v granted=%v", owned, granted)
	}
	owned, granted = SecretNames("web", secrets, nil)
	if strings.Join(owned, ",") != "jk-secret-b,jk-secret-b-1,jk-secret-c,jk-secret-c-1,jk-secret-d" || strings.Join(granted, ",") != "jk-secret-a,jk-secret-a-1,jk-secret-a-2" {
		t.Errorf("web owned=%v granted=%v", owned, granted)
	}
	owned, granted = SecretNames("nobody", secrets, nil)
	if len(owned)+len(granted) != 0 {
		t.Error("nobody has secrets")
	}

	if r := SecretRules(nil, nil); len(r) != 0 {
		t.Errorf("no names, no rules: %+v", r)
	}
	r := SecretRules([]string{"x"}, []string{"y"})
	if len(r) != 2 || r[0].ResourceNames[0] != "x" || len(r[0].Verbs) != 6 || r[1].ResourceNames[0] != "y" || strings.Join(r[1].Verbs, ",") != "get,list,watch" {
		t.Errorf("rules %+v", r)
	}
	for _, rule := range r {
		if len(rule.ResourceNames) == 0 {
			t.Error("a rule without names would grant every secret")
		}
	}
}

func TestSecretRequests(t *testing.T) {
	s := sec("jk-secret-a", "a", "pg", `[{"application":"web"},{"application":"pg"}]`)
	s.Namespace = "db"
	reqs := secretRequests(&s)
	names := map[string]bool{}
	for _, r := range reqs {
		if r.Namespace != "db" {
			t.Errorf("namespace %q", r.Namespace)
		}
		names[r.Name] = true
	}
	if !names["pg"] || !names["web"] {
		t.Errorf("requests %v", reqs)
	}
	if got := secretRequests(&corev1.ConfigMap{}); len(got) != 0 {
		t.Errorf("no app: %v", got)
	}
}

func TestSecretCacheOptions(t *testing.T) {
	opts := CacheOptions()
	var found bool
	for obj, by := range opts.ByObject {
		if _, ok := obj.(*corev1.Secret); ok {
			found = by.Label.Matches(labels.Set{v1alpha1.SecretLabel: "x"}) && !by.Label.Matches(labels.Set{"other": "x"})
		}
	}
	if !found {
		t.Error("Secret cache must be limited to jk secrets")
	}
}

func TestPVCNameAndVolumeOwnership(t *testing.T) {
	storages := []string{"pgdata", "logs"}
	for name, want := range map[string]bool{
		"pg-pgdata-1a2b3c4d-pg-0": true, "pg-logs-1a2b3c4d-pg-12": true, "pg-pgdata-1a2b3c4d-pg-": false, "pg-pgdata-1a2b3c4d-pg-x": false,
		"pg-other-1a2b3c4d-pg-0": false, "pgx-pgdata-1a2b3c4d-pgx-0": false, "pg-pgdata-1a2b3c4d-pg-0-1": false, "pg-pgdata-pg-0": false,
	} {
		if PVCNameMatches("pg", storages, name) != want {
			t.Errorf("PVCNameMatches(%q) = %v", name, !want)
		}
	}
	// "db" and "db-x" must not claim each other's volumes.
	if PVCNameMatches("db", []string{"data"}, "db-x-data-1a2b3c4d-db-x-0") {
		t.Error("db claims db-x's volume")
	}
	labelled := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1alpha1.AppLabel: "pg", v1alpha1.NamespaceLabel: "db"}}}
	claimed := &corev1.PersistentVolume{Spec: corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{Namespace: "db", Name: "pg-pgdata-1a2b3c4d-pg-1"}}}
	wrongNS := &corev1.PersistentVolume{Spec: corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{Namespace: "other", Name: "pg-pgdata-1a2b3c4d-pg-1"}}}
	wrongLabel := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1alpha1.AppLabel: "pg", v1alpha1.NamespaceLabel: "other"}}}
	for pv, want := range map[*corev1.PersistentVolume]bool{labelled: true, claimed: true, wrongNS: false, wrongLabel: false, {}: false} {
		if OwnsVolume(pv, "pg", "db", storages) != want {
			t.Errorf("OwnsVolume(%+v) = %v", pv, !want)
		}
	}
}

func TestStorageNames(t *testing.T) {
	app := testApp(pgMetadata)
	if got := storageNames(app); len(got) != 1 || got[0] != "pgdata" {
		t.Errorf("%v", got)
	}
	app.Status.Charm.Metadata = nil
	if storageNames(app) != nil {
		t.Error("no metadata")
	}
	app.Status.Charm = nil
	if storageNames(app) != nil {
		t.Error("no charm")
	}
	app.Status.Charm = &v1alpha1.ResolvedCharm{Metadata: &v1alpha1.JSON{Raw: []byte("[")}}
	if storageNames(app) != nil {
		t.Error("bad metadata")
	}
}

func node(arch string, unschedulable bool, taint corev1.TaintEffect) corev1.Node {
	n := corev1.Node{}
	n.Status.NodeInfo.Architecture = arch
	n.Spec.Unschedulable = unschedulable
	if taint != "" {
		n.Spec.Taints = []corev1.Taint{{Key: "k", Effect: taint}}
	}
	return n
}

func TestClusterArchitecturesAndChoice(t *testing.T) {
	labelled := corev1.Node{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{archLabel: "s390x"}}}
	tests := []struct {
		name  string
		nodes []corev1.Node
		want  string
	}{
		{"none", nil, ""},
		{"one", []corev1.Node{node("arm64", false, "")}, "arm64"},
		{"same twice", []corev1.Node{node("arm64", false, ""), node("arm64", false, "")}, "arm64"},
		{"mixed", []corev1.Node{node("arm64", false, ""), node("amd64", false, "")}, "amd64,arm64"},
		{"tainted control plane ignored", []corev1.Node{node("amd64", false, corev1.TaintEffectNoSchedule), node("arm64", false, "")}, "arm64"},
		{"cordoned ignored", []corev1.Node{node("amd64", true, ""), node("arm64", false, "")}, "arm64"},
		{"prefer-no-schedule counts", []corev1.Node{node("amd64", false, corev1.TaintEffectPreferNoSchedule)}, "amd64"},
		{"only unusable nodes: use them", []corev1.Node{node("amd64", true, "")}, "amd64"},
		{"label fallback", []corev1.Node{labelled}, "s390x"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := strings.Join(ClusterArchitectures(tt.nodes), ","); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
	if a, err := ChooseArchitecture("riscv64", []string{"amd64", "arm64"}); err != nil || a != "riscv64" {
		t.Errorf("constraint: %q %v", a, err)
	}
	if a, err := ChooseArchitecture("", []string{"arm64"}); err != nil || a != "arm64" {
		t.Errorf("single: %q %v", a, err)
	}
	if _, err := ChooseArchitecture("", []string{"amd64", "arm64"}); !errors.Is(err, errMixedArch) || !strings.Contains(err.Error(), "constraints.arch") {
		t.Errorf("mixed: %v", err)
	}
	if _, err := ChooseArchitecture("", nil); err == nil {
		t.Error("no nodes")
	}
}

func TestHubError(t *testing.T) {
	req := charmhub.Request{Name: "pg", Channel: "stable", Base: charmhub.Base{Architecture: "amd64"}}
	err := hubError("pg", req, &charmhub.Error{Code: charmhub.CodeRevisionNotFound, Message: "none", Releases: []charmhub.Release{
		{Channel: "14/stable", Base: charmhub.Base{Name: "ubuntu", Channel: "22.04", Architecture: "amd64"}},
		{Channel: "14/stable", Base: charmhub.Base{Name: "ubuntu", Channel: "22.04", Architecture: "amd64"}},
		{Channel: "16/stable", Base: charmhub.Base{Name: "ubuntu", Channel: "24.04", Architecture: "arm64"}},
	}})
	if !errors.Is(err, errCharmNotFound) || !strings.Contains(err.Error(), "latest/stable") || !strings.Contains(err.Error(), "14/stable (ubuntu@22.04 amd64), 16/stable (ubuntu@24.04 arm64)") {
		t.Errorf("not found: %v", err)
	}
	if err := hubError("pg", req, errors.New("dial tcp: refused")); !errors.Is(err, errCharmUnavailable) {
		t.Errorf("transient: %v", err)
	}
	if err := hubError("pg", req, &charmhub.Error{Code: charmhub.CodeNameNotFound, Message: "x"}); !errors.Is(err, errCharmNotFound) || strings.Contains(err.Error(), "available") {
		t.Errorf("name: %v", err)
	}
}

func TestParseCharmFiles(t *testing.T) {
	md, cfg, act, err := parseCharmFiles(&charmhub.Charm{MetadataYAML: "name: x\nassumes: [k8s-api]\n", ConfigYAML: "options:\n  a:\n    default: 1\n", ActionsYAML: "  \n"})
	if err != nil || md == nil || cfg == nil || act != nil {
		t.Fatalf("%v %v %v %v", md, cfg, act, err)
	}
	if !strings.Contains(string(md.Raw), `"assumes":["k8s-api"]`) {
		t.Errorf("metadata %s", md.Raw)
	}
	if _, _, _, err := parseCharmFiles(&charmhub.Charm{}); err == nil || !strings.Contains(err.Error(), "no metadata.yaml") {
		t.Errorf("missing metadata: %v", err)
	}
	if _, _, _, err := parseCharmFiles(&charmhub.Charm{MetadataYAML: "name: [\n"}); err == nil {
		t.Error("bad yaml")
	}
	if _, _, _, err := parseCharmFiles(&charmhub.Charm{MetadataYAML: "name: x", ConfigYAML: ": ["}); err == nil {
		t.Error("bad config")
	}
	if _, _, _, err := parseCharmFiles(&charmhub.Charm{MetadataYAML: "name: x", ActionsYAML: ": ["}); err == nil {
		t.Error("bad actions")
	}
}

func TestVerifiedCache(t *testing.T) {
	var v verifiedCache
	now := time.Now()
	if v.fresh("d", now) {
		t.Error("unknown digest is not fresh")
	}
	v.mark("d", now)
	if !v.fresh("d", now.Add(verifiedTTL-time.Second)) || v.fresh("d", now.Add(verifiedTTL+time.Second)) || v.fresh("other", now) {
		t.Error("ttl")
	}
}

func TestTrustRulesAndNames(t *testing.T) {
	if len(TrustRules(v1alpha1.TrustNone, "db")) != 3 || len(TrustRules("", "db")) != 3 {
		t.Error("default rules")
	}
	for _, tr := range []v1alpha1.Trust{v1alpha1.TrustNamespace, v1alpha1.TrustCluster} {
		r := TrustRules(tr, "db")
		if len(r) != 1 || r[0].Verbs[0] != "*" {
			t.Errorf("%s: %+v", tr, r)
		}
	}
	if ClusterObjectName("db", "pg") != "jk-db-pg" {
		t.Error("name")
	}
	l := ClusterLabels("db", "pg")
	if l[v1alpha1.AppLabel] != "pg" || l[v1alpha1.NamespaceLabel] != "db" {
		t.Errorf("%v", l)
	}
}

func fakeClient(objs ...client.Object) client.Client {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = v1alpha1.AddToScheme(s)
	return fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).WithStatusSubresource(&v1alpha1.Relation{}).Build()
}

func TestAllocateID(t *testing.T) {
	ctx := context.Background()
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "db", Name: v1alpha1.ModelConfigMap}, Data: map[string]string{RelationIDKey: "4"}}
	c := fakeClient(cm)
	for want := int64(5); want < 8; want++ {
		id, err := AllocateID(ctx, c, c, "db", RelationIDKey)
		if err != nil || id != want {
			t.Fatalf("id %d, %v; want %d", id, err, want)
		}
	}
	// Another counter starts at 1; a missing key counts as 0.
	if id, err := AllocateID(ctx, c, c, "db", ActionIDKey); err != nil || id != 1 {
		t.Errorf("action id %d %v", id, err)
	}
	var got corev1.ConfigMap
	_ = c.Get(ctx, client.ObjectKeyFromObject(cm), &got)
	if got.Data[RelationIDKey] != "7" || got.Data[ActionIDKey] != "1" {
		t.Errorf("counters %v", got.Data)
	}
	if _, err := AllocateID(ctx, c, c, "nowhere", RelationIDKey); err == nil {
		t.Error("missing ConfigMap")
	}
	bad := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "bad", Name: v1alpha1.ModelConfigMap}, Data: map[string]string{RelationIDKey: "x"}}
	if _, err := AllocateID(ctx, fakeClient(bad), fakeClient(bad), "bad", RelationIDKey); err == nil {
		t.Error("corrupt counter")
	}
	neg := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "neg", Name: v1alpha1.ModelConfigMap}, Data: map[string]string{RelationIDKey: "-3"}}
	if _, err := AllocateID(ctx, fakeClient(neg), fakeClient(neg), "neg", RelationIDKey); err == nil {
		t.Error("negative counter")
	}
}

// Concurrent allocators must never hand out the same id (optimistic concurrency on the counter).
func TestAllocateIDConcurrent(t *testing.T) {
	ctx := context.Background()
	c := fakeClient(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "db", Name: v1alpha1.ModelConfigMap}})
	const n = 20
	ids := make(chan int64, n)
	for i := 0; i < n; i++ {
		go func() {
			var id int64
			var err error
			for try := 0; try < 50; try++ { // the fake client reports conflicts; DefaultRetry gives up after 5
				if id, err = AllocateID(ctx, c, c, "db", RelationIDKey); err == nil {
					break
				}
			}
			if err != nil {
				id = -1
			}
			ids <- id
		}()
	}
	seen := map[int64]bool{}
	for i := 0; i < n; i++ {
		id := <-ids
		if id < 1 || seen[id] {
			t.Fatalf("bad or duplicate id %d", id)
		}
		seen[id] = true
	}
}
