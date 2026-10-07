package operator

import (
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/yaml"

	"github.com/luci1900/jk/api/v1alpha1"
)

var update = flag.Bool("update", false, "rewrite golden files")

const pgMetadata = `{
 "name": "pg",
 "containers": {
  "postgresql": {"resource": "postgresql-image", "mounts": [{"storage": "pgdata", "location": "/var/lib/postgresql/data"}]},
  "exporter": {"resource": "exporter-image"}
 },
 "resources": {
  "postgresql-image": {"type": "oci-image", "upstream-source": "ghcr.io/canonical/charmed-postgresql@sha256:11db"},
  "exporter-image": {"type": "oci-image", "upstream-source": "example.com/exporter:1"},
  "a-file": {"type": "file"}
 },
 "peers": {"database-peers": {"interface": "postgresql_peers"}, "restart": {"interface": "rolling_op"}, "my_peers": {"interface": "x"}},
 "storage": {"pgdata": {"type": "filesystem", "location": "/var/lib/postgresql/data", "minimum-size": "2G"}}
}`

func testApp(md string) *v1alpha1.Application {
	scale := int32(2)
	app := &v1alpha1.Application{
		ObjectMeta: metav1.ObjectMeta{Name: "pg", Namespace: "db", UID: types.UID("app-uid")},
		Spec: v1alpha1.ApplicationSpec{
			Charm: v1alpha1.CharmSpec{Name: "pg", Source: "local", Sha256: "sha256:abc"},
			Scale: &scale,
		},
	}
	app.Status.Charm = &v1alpha1.ResolvedCharm{
		Base:     "ubuntu@22.04",
		Sha256:   "sha256:abc",
		Image:    "jk-registry.jk-system.svc:5000/charms@sha256:abc",
		Metadata: &v1alpha1.JSON{Raw: []byte(md)},
	}
	md2, _ := parseMetadata(app.Status.Charm.Metadata)
	app.Status.Charm.ResourceImages = ResourceImages(md2, app.Spec.Resources)
	return app
}

func testConfig() Config { return DefaultConfig("kind.local/jk-agent:dev") }

func golden(t *testing.T, name string, objs ...any) {
	t.Helper()
	var sb strings.Builder
	for i, o := range objs {
		b, err := yaml.Marshal(o)
		if err != nil {
			t.Fatal(err)
		}
		if i > 0 {
			sb.WriteString("---\n")
		}
		sb.Write(b)
	}
	path := filepath.Join("testdata", name+".golden.yaml")
	if *update {
		if err := os.WriteFile(path, []byte(sb.String()), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v (run with -update)", err)
	}
	if string(want) != sb.String() {
		t.Errorf("%s differs from golden file (run with -update to accept):\n%s", name, sb.String())
	}
}

func TestBuildGolden(t *testing.T) {
	app := testApp(pgMetadata)
	app.Spec.Resources = map[string]string{"exporter-image": "example.com/exporter:2"}
	app.Status.Charm.ResourceImages = ResourceImages(mustMD(t, pgMetadata), app.Spec.Resources)
	app.Spec.Trust = v1alpha1.TrustNamespace
	d, err := Build(app, Inputs{
		NamespaceUID: "ns-uid",
		OpenedPorts:  []v1alpha1.PortRange{{Protocol: "tcp", From: 5432, To: 5432}, {Protocol: "tcp", From: 8008, To: 8008}},
		OwnedSecrets: []string{"jk-secret-abc", "jk-secret-abc-1"}, GrantedSecrets: []string{"jk-secret-def"},
	}, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	var objs []any
	for _, o := range d.Objects() {
		objs = append(objs, o)
	}
	golden(t, "postgres", objs...)
}

func mustMD(t *testing.T, s string) *charmMetadata {
	t.Helper()
	md, err := parseMetadata(&v1alpha1.JSON{Raw: []byte(s)})
	if err != nil {
		t.Fatal(err)
	}
	return md
}

func TestBuildSimpleCharmGolden(t *testing.T) {
	app := testApp(`{"name":"simple","containers":{"workload":{"resource":"img"}},"resources":{"img":{"type":"oci-image","upstream-source":"busybox:1"}}}`)
	d, err := Build(app, Inputs{NamespaceUID: "ns-uid"}, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	golden(t, "simple", d.StatefulSet)
}

func TestBuildCharmWithoutContainers(t *testing.T) {
	app := testApp(`{"name":"data-integrator","requires":{"postgresql":{"interface":"postgresql_client","limit":1}}}`)
	d, err := Build(app, Inputs{NamespaceUID: "ns-uid"}, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	pod := d.StatefulSet.Spec.Template.Spec
	if len(pod.Containers) != 1 || pod.Containers[0].Name != "charm" || len(pod.InitContainers) != 1 {
		t.Fatalf("containers: %d (%v), init %d", len(pod.Containers), pod.Containers, len(pod.InitContainers))
	}
	for _, c := range append(pod.InitContainers, pod.Containers...) {
		for _, e := range c.Env {
			if e.Name == "JUJU_CONTAINER_NAMES" {
				t.Errorf("%s sets JUJU_CONTAINER_NAMES=%q", c.Name, e.Value)
			}
		}
	}
}

func TestBuildInvariants(t *testing.T) {
	app := testApp(pgMetadata)
	d, err := Build(app, Inputs{NamespaceUID: "ns-uid"}, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range d.Objects() {
		refs := o.GetOwnerReferences()
		if len(refs) != 1 || refs[0].UID != "app-uid" || refs[0].Controller == nil || !*refs[0].Controller || refs[0].Kind != "Application" {
			t.Errorf("%s: owner refs %+v", o.GetName(), refs)
		}
		if o.GetLabels()[v1alpha1.AppLabel] != "pg" || o.GetLabels()[nameLabel] != "pg" || o.GetNamespace() != "db" {
			t.Errorf("%s: labels/namespace %v %s", o.GetName(), o.GetLabels(), o.GetNamespace())
		}
	}
	sts := d.StatefulSet
	if *sts.Spec.Replicas != 2 {
		t.Errorf("replicas %d", *sts.Spec.Replicas)
	}
	pod := sts.Spec.Template
	if pod.Labels[nameLabel] != "pg" || pod.Labels[v1alpha1.AppLabel] != "pg" {
		t.Errorf("pod labels %v", pod.Labels)
	}
	if got := sts.Spec.Selector.MatchLabels; len(got) != 1 || got[nameLabel] != "pg" {
		t.Errorf("selector %v", got)
	}
	// Containers: charm first, then workloads by name, with distinct pebble ports.
	var names []string
	for _, c := range pod.Spec.Containers {
		names = append(names, c.Name)
	}
	if strings.Join(names, ",") != "charm,exporter,postgresql" {
		t.Errorf("containers %v", names)
	}
	if got := pod.Spec.Containers[1].Args[len(pod.Spec.Containers[1].Args)-2]; got != ":38813" {
		t.Errorf("exporter port arg %q", got)
	}
	if got := pod.Spec.Containers[2].Args[len(pod.Spec.Containers[2].Args)-2]; got != ":38814" {
		t.Errorf("postgresql port arg %q", got)
	}
	// Identity env on both charm-init and charm.
	for _, c := range []struct {
		name string
		env  map[string]string
	}{{"init", envMap(pod.Spec.InitContainers[0].Env)}, {"charm", envMap(pod.Spec.Containers[0].Env)}} {
		for k, v := range map[string]string{"JK_APP": "pg", "JK_NAMESPACE": "db", "JK_MODEL_NAME": "db", "JK_MODEL_UUID": "ns-uid", "JUJU_CONTAINER_NAMES": "exporter,postgresql"} {
			if c.env[k] != v {
				t.Errorf("%s env %s = %q, want %q", c.name, k, c.env[k], v)
			}
		}
		if _, ok := c.env["JK_POD_NAME"]; !ok {
			t.Errorf("%s: no JK_POD_NAME", c.name)
		}
	}
	init := pod.Spec.InitContainers[0]
	if init.Name != "charm-init" || init.Image != "kind.local/jk-agent:dev" || init.Args[0] != "init" {
		t.Errorf("init container %+v", init)
	}
	if got := init.Args[len(init.Args)-1]; got != "--charm-image=jk-registry.jk-system.svc:5000/charms@sha256:abc" {
		t.Errorf("charm image arg %q", got)
	}
	if pod.Spec.Containers[0].Image != "ghcr.io/juju/charm-base:ubuntu-22.04" {
		t.Errorf("charm image %q", pod.Spec.Containers[0].Image)
	}
	// Storage: claim of 2Gi (minimum-size 2G), mounted in the charm and workload containers.
	if len(sts.Spec.VolumeClaimTemplates) != 1 || sts.Spec.VolumeClaimTemplates[0].Name != "pg-pgdata-00000000" {
		t.Fatalf("claims %+v", sts.Spec.VolumeClaimTemplates)
	}
	if q := sts.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests["storage"]; q.Cmp(resource.MustParse("2Gi")) != 0 {
		t.Errorf("claim size %s", q.String())
	}
	// The storage declares a location, so the charm container mounts it there too (postgresql-k8s writes patroni.yml
	// into it from the charm container), not under /var/lib/juju/storage.
	if !hasMount(pod.Spec.Containers[0], "/var/lib/postgresql/data") || hasMount(pod.Spec.Containers[0], "/var/lib/juju/storage/pgdata/0") || !hasMount(pod.Spec.Containers[2], "/var/lib/postgresql/data") {
		t.Error("storage not mounted")
	}
}

func envMap(env []corev1.EnvVar) map[string]string {
	m := map[string]string{}
	for _, e := range env {
		m[e.Name] = e.Value
	}
	return m
}

func TestBuildOptions(t *testing.T) {
	app := testApp(pgMetadata)
	cpu := 250
	class := "fast"
	app.Spec.Constraints = &v1alpha1.Constraints{Mem: "512M", CPUPower: &cpu, Arch: "arm64"}
	app.Spec.Storage = map[string]v1alpha1.StorageSpec{"pgdata": {Size: "10G", StorageClass: &class}}
	d, err := Build(app, Inputs{NamespaceUID: "u"}, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	tpl := d.StatefulSet.Spec.Template.Spec
	if tpl.NodeSelector["kubernetes.io/arch"] != "arm64" {
		t.Errorf("node selector %v", tpl.NodeSelector)
	}
	req := tpl.Containers[2].Resources.Requests
	if m := req["memory"]; m.Cmp(resource.MustParse("512Mi")) != 0 {
		t.Errorf("memory %s", m.String())
	}
	if c := req["cpu"]; c.MilliValue() != 250 {
		t.Errorf("cpu %s", c.String())
	}
	if len(tpl.Containers[0].Resources.Requests) != 0 {
		t.Error("charm container must not get workload constraints")
	}
	pvc := d.StatefulSet.Spec.VolumeClaimTemplates[0]
	if q := pvc.Spec.Resources.Requests["storage"]; q.Cmp(resource.MustParse("10Gi")) != 0 || *pvc.Spec.StorageClassName != "fast" {
		t.Errorf("pvc %+v", pvc.Spec)
	}
	zero := int32(0)
	app.Spec.Scale = &zero
	d, _ = Build(app, Inputs{NamespaceUID: "u"}, testConfig())
	if *d.StatefulSet.Spec.Replicas != 0 {
		t.Error("scale 0 not honoured")
	}
	app.Spec.Scale = nil
	d, _ = Build(app, Inputs{NamespaceUID: "u"}, testConfig())
	if *d.StatefulSet.Spec.Replicas != 1 {
		t.Error("default scale is 1")
	}
}

func TestBuildErrors(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*v1alpha1.Application)
		cfg    func(*Config)
		want   string
		inval  bool
	}{
		{"no charm", func(a *v1alpha1.Application) { a.Status.Charm = nil }, nil, "not resolved", true},
		{"no metadata", func(a *v1alpha1.Application) { a.Status.Charm.Metadata = nil }, nil, "no metadata", true},
		{"bad metadata", func(a *v1alpha1.Application) { a.Status.Charm.Metadata = &v1alpha1.JSON{Raw: []byte("[1]")} }, nil, "parsing", true},
		{"no agent image", nil, func(c *Config) { c.AgentImage = "" }, "JK_AGENT_IMAGE", false},
		{"missing resource image", func(a *v1alpha1.Application) { a.Status.Charm.ResourceImages = nil }, nil, "has no image", true},
		{"block storage", func(a *v1alpha1.Application) {
			a.Status.Charm.Metadata = &v1alpha1.JSON{Raw: []byte(`{"storage":{"d":{"type":"block"}}}`)}
		}, nil, "only filesystem", true},
		{"bad min size", func(a *v1alpha1.Application) {
			a.Status.Charm.Metadata = &v1alpha1.JSON{Raw: []byte(`{"storage":{"d":{"type":"filesystem","minimum-size":"lots"}}}`)}
		}, nil, "minimum-size", true},
		{"bad spec size", func(a *v1alpha1.Application) {
			a.Spec.Storage = map[string]v1alpha1.StorageSpec{"pgdata": {Size: "x"}}
		}, nil, "spec.storage", true},
		{"bad mem", func(a *v1alpha1.Application) { a.Spec.Constraints = &v1alpha1.Constraints{Mem: "x"} }, nil, "constraints.mem", true},
		{"unknown storage mount", func(a *v1alpha1.Application) {
			a.Status.Charm.Metadata = &v1alpha1.JSON{Raw: []byte(`{"containers":{"c":{"resource":"r","mounts":[{"storage":"nope"}]}},"resources":{"r":{"type":"oci-image","upstream-source":"x"}}}`)}
			a.Status.Charm.ResourceImages = map[string]string{"r": "x"}
		}, nil, "unknown storage", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := testApp(pgMetadata)
			if tt.mutate != nil {
				tt.mutate(app)
			}
			cfg := testConfig()
			if tt.cfg != nil {
				tt.cfg(&cfg)
			}
			_, err := Build(app, Inputs{NamespaceUID: "u"}, cfg)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
			if errors.Is(err, errInvalid) != tt.inval {
				t.Errorf("errInvalid = %v, want %v", !tt.inval, tt.inval)
			}
		})
	}
}

func TestAgentRules(t *testing.T) {
	// The agent may write only unitdata/appdata, update leases, patch pods, create secrets and report action status.
	writes := map[string]bool{}
	for _, r := range AgentRules() {
		for _, res := range r.Resources {
			for _, v := range r.Verbs {
				if v != "get" && v != "list" && v != "watch" {
					writes[res+":"+v] = true
				}
			}
		}
	}
	for _, w := range []string{"unitdata:create", "unitdata:update", "unitdata:patch", "appdata:create", "leases:update", "pods:patch", "events:create", "secrets:create", "actions/status:update", "actions/status:patch"} {
		if !writes[w] {
			t.Errorf("missing %s", w)
		}
	}
	if len(writes) != 12 {
		t.Errorf("unexpected writes: %v", writes)
	}
}

func hasMount(c corev1.Container, path string) bool {
	for _, m := range c.VolumeMounts {
		if m.MountPath == path {
			return true
		}
	}
	return false
}

func TestBuildTrust(t *testing.T) {
	tests := []struct {
		trust       v1alpha1.Trust
		fullAccess  bool
		clusterRole bool
	}{
		{"", false, false},
		{v1alpha1.TrustNone, false, false},
		{v1alpha1.TrustNamespace, true, false},
		{v1alpha1.TrustCluster, true, true},
	}
	for _, tt := range tests {
		t.Run(string(tt.trust), func(t *testing.T) {
			app := testApp(pgMetadata)
			app.Spec.Trust = tt.trust
			d, err := Build(app, Inputs{NamespaceUID: "u"}, testConfig())
			if err != nil {
				t.Fatal(err)
			}
			var star, exec, nsRule bool
			for _, r := range d.Role.Rules {
				star = star || (len(r.Resources) == 1 && r.Resources[0] == "*" && r.Verbs[0] == "*")
				for _, res := range r.Resources {
					exec = exec || res == "pods/exec"
				}
				nsRule = nsRule || (len(r.ResourceNames) == 1 && r.ResourceNames[0] == "db")
			}
			if star != tt.fullAccess || exec == tt.fullAccess || nsRule == tt.fullAccess {
				t.Errorf("star=%v exec=%v nsRule=%v; want fullAccess=%v", star, exec, nsRule, tt.fullAccess)
			}
			// The agent's own rules are always there.
			if !hasRule(d.Role.Rules, "unitdata", "update") {
				t.Error("agent rules missing")
			}
			if (d.ClusterRole != nil) != tt.clusterRole {
				t.Fatalf("cluster role present = %v", d.ClusterRole != nil)
			}
			if !tt.clusterRole {
				if len(d.ClusterObjects()) != 0 {
					t.Error("unexpected cluster objects")
				}
				return
			}
			cr, crb := d.ClusterRole, d.ClusterRoleBinding
			if cr.Name != "jk-db-pg" || crb.RoleRef.Name != cr.Name || crb.Subjects[0].Name != "pg" || crb.Subjects[0].Namespace != "db" {
				t.Errorf("cluster objects: %+v %+v", cr.ObjectMeta, crb)
			}
			for _, o := range d.ClusterObjects() {
				if len(o.GetOwnerReferences()) != 0 {
					t.Errorf("%s: cluster-scoped objects cannot have namespaced owners", o.GetName())
				}
				if l := o.GetLabels(); l[v1alpha1.AppLabel] != "pg" || l[v1alpha1.NamespaceLabel] != "db" {
					t.Errorf("%s labels %v", o.GetName(), l)
				}
			}
		})
	}
}

func hasRule(rules []rbacv1.PolicyRule, resource, verb string) bool {
	for _, r := range rules {
		for _, res := range r.Resources {
			for _, v := range r.Verbs {
				if res == resource && v == verb {
					return true
				}
			}
		}
	}
	return false
}

func TestBuildPeerRelations(t *testing.T) {
	d, err := Build(testApp(pgMetadata), Inputs{NamespaceUID: "u"}, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, r := range d.PeerRelations {
		got = append(got, r.Name+"="+r.Spec.Endpoints[0].Endpoint)
		if len(r.Spec.Endpoints) != 1 || r.Spec.Endpoints[0].Application != "pg" || r.Spec.Endpoints[0].Namespace != "db" {
			t.Errorf("endpoints %+v", r.Spec.Endpoints)
		}
		if r.Labels[v1alpha1.PeerEndpointLabel] != r.Spec.Endpoints[0].Endpoint || r.Labels[v1alpha1.AppLabel] != "pg" {
			t.Errorf("labels %v", r.Labels)
		}
		if len(r.OwnerReferences) != 1 {
			t.Errorf("owner %v", r.OwnerReferences)
		}
	}
	want := "pg.database-peers=database-peers pg.my-peers=my_peers pg.restart=restart"
	if strings.Join(got, " ") != want {
		t.Errorf("peer relations %v, want %s", got, want)
	}
	// No peers, no relations.
	d, _ = Build(testApp(`{"name":"x"}`), Inputs{NamespaceUID: "u"}, testConfig())
	if len(d.PeerRelations) != 0 {
		t.Error("unexpected relations")
	}
}

func TestBuildPortsAndSecrets(t *testing.T) {
	app := testApp(pgMetadata)
	d, _ := Build(app, Inputs{NamespaceUID: "u"}, testConfig())
	if p := d.Service.Spec.Ports; len(p) != 1 || p[0].Name != "placeholder" || p[0].Port != 65535 {
		t.Errorf("placeholder: %+v", p)
	}
	if len(d.SecretsRole.Rules) != 0 {
		t.Errorf("secrets role should be empty: %+v", d.SecretsRole.Rules)
	}
	d, _ = Build(app, Inputs{NamespaceUID: "u", OpenedPorts: []v1alpha1.PortRange{{Protocol: "tcp", From: 5432, To: 5432}}, OwnedSecrets: []string{"a"}, GrantedSecrets: []string{"b"}}, testConfig())
	if p := d.Service.Spec.Ports; len(p) != 1 || p[0].Name != "juju-5432-tcp" || p[0].Port != 5432 || p[0].TargetPort.IntVal != 5432 {
		t.Errorf("ports: %+v", p)
	}
	if len(d.SecretsRole.Rules) != 2 || d.SecretsRoleBinding.RoleRef.Name != "pg-jk-secrets" || d.SecretsRoleBinding.Subjects[0].Name != "pg" {
		t.Errorf("secrets role: %+v %+v", d.SecretsRole.Rules, d.SecretsRoleBinding)
	}
}

// The operator must not own StatefulSet fields that charms patch (postgresql-k8s sets the rollout partition):
// the apply configuration must leave updateStrategy out entirely.
func TestStatefulSetLeavesCharmPatchedFieldsAlone(t *testing.T) {
	d, err := Build(testApp(pgMetadata), Inputs{NamespaceUID: "u"}, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	m, err := applyConfiguration(d.StatefulSet)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := m["status"]; ok {
		t.Error("status must not be applied")
	}
	spec := m["spec"].(map[string]any)
	for _, f := range []string{"updateStrategy", "revisionHistoryLimit", "minReadySeconds", "ordinals"} {
		if _, ok := spec[f]; ok {
			t.Errorf("spec.%s is set by the operator: %v", f, spec[f])
		}
	}
}

func TestBuildArchitectureSelector(t *testing.T) {
	app := testApp(pgMetadata)
	app.Status.Charm.Architecture = "arm64"
	d, _ := Build(app, Inputs{NamespaceUID: "u"}, testConfig())
	if got := d.StatefulSet.Spec.Template.Spec.NodeSelector; got["kubernetes.io/arch"] != "arm64" {
		t.Errorf("resolved architecture: %v", got)
	}
	app.Spec.Constraints = &v1alpha1.Constraints{Arch: "amd64"}
	d, _ = Build(app, Inputs{NamespaceUID: "u"}, testConfig())
	if got := d.StatefulSet.Spec.Template.Spec.NodeSelector; got["kubernetes.io/arch"] != "amd64" {
		t.Errorf("constraint wins: %v", got)
	}
	app = testApp(pgMetadata)
	d, _ = Build(app, Inputs{NamespaceUID: "u"}, testConfig())
	if got := d.StatefulSet.Spec.Template.Spec.NodeSelector; got != nil {
		t.Errorf("local charm without constraint: %v", got)
	}
}

// Recent kubelets make the service account token 0600 and owned by the runAsUser when every container sets the same
// runAsUser, so workload processes running as another user can't read it. Keep at least one container unset.
func TestTokenStaysReadableByWorkloadUsers(t *testing.T) {
	app := testApp(`{"name":"simple","containers":{"workload":{"resource":"img"}},"resources":{"img":{"type":"oci-image","upstream-source":"busybox:1"}}}`)
	d, err := Build(app, Inputs{NamespaceUID: "ns-uid"}, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	unset := 0
	for _, c := range d.StatefulSet.Spec.Template.Spec.Containers {
		if c.SecurityContext == nil || c.SecurityContext.RunAsUser == nil {
			unset++
		}
	}
	if unset == 0 {
		t.Fatal("every container sets runAsUser: the kubelet would make the service account token readable only by that user")
	}
}
