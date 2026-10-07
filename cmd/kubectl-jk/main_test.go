package main

import (
	"bytes"
	"context"
	"regexp"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/kubernetes"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/pkg/charmhub"
	"github.com/luci1900/jk/pkg/sdk"
)

func TestCommandsRegistered(t *testing.T) {
	root := newRoot()
	for _, name := range []string{
		"install", "uninstall", "version", "add-model", "destroy-model", "models", "switch", "model-config", "find", "info",
		"deploy", "refresh", "remove-application", "config", "trust", "scale-application", "add-unit", "remove-unit",
		"integrate", "remove-relation", "status", "resolved", "actions", "run", "exec", "operations", "show-operation", "show-task",
		"show-application", "show-unit", "show-model", "constraints", "set-constraints", "ssh", "scp", "debug-log",
	} {
		if c, _, err := root.Find([]string{name}); err != nil || c.Name() != name {
			t.Errorf("missing command %q (%v)", name, err)
		}
	}
	// kubectl's connection flags are available everywhere, and -m is the model.
	for _, f := range []string{"context", "kubeconfig", "model"} {
		if root.PersistentFlags().Lookup(f) == nil {
			t.Errorf("missing flag --%s", f)
		}
	}
	if root.PersistentFlags().ShorthandLookup("m") == nil {
		t.Error("no -m")
	}
	// --namespace is a hidden alias of --model, and -n is only juju's --num-units.
	if f := root.PersistentFlags().Lookup("namespace"); f == nil || !f.Hidden || f.Shorthand != "" {
		t.Errorf("--namespace: %+v", f)
	}
	for name, want := range map[string]string{"status": "", "config": "", "deploy": "num-units", "add-unit": "num-units"} {
		c, _, _ := root.Find([]string{name})
		got := ""
		if f := c.Flags().ShorthandLookup("n"); f != nil {
			got = f.Name
		}
		if got != want {
			t.Errorf("%s: -n is %q, want %q", name, got, want)
		}
	}
}

func TestVersion(t *testing.T) {
	root := newRoot()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetArgs([]string{"version"})
	if err := root.Execute(); err != nil || strings.TrimSpace(out.String()) == "" {
		t.Fatalf("version: %q %v", out.String(), err)
	}
}

func TestUnsupportedCommands(t *testing.T) {
	for name, want := range map[string]string{
		"bootstrap": "use `kubectl jk install`", "expose": "use an ingress charm", "whoami": "kubectl auth whoami",
		"logout": `"logout" is not supported in jk`,
	} {
		root := newRoot()
		root.SetArgs([]string{name, "--whatever"})
		root.SetErr(&bytes.Buffer{})
		err := root.Execute()
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: %v, want %q", name, err, want)
		}
	}
	// Every unsupported name is a real juju command and none shadows one jk implements.
	root := newRoot()
	seen := map[string]int{}
	for _, c := range root.Commands() {
		seen[c.Name()]++
	}
	for name, n := range seen {
		if n > 1 {
			t.Errorf("command %q registered %d times", name, n)
		}
	}
}

// fakeEnv runs commands against a fake cluster: one model "m" with the given objects.
type fakeEnv struct {
	t    *testing.T
	kube client.Client
	hub  *hub
	in   string
	// kubectl collects the arguments ssh and scp pass to kubectl; pods are what debug-log reads logs of.
	kubectl [][]string
	pods    []runtime.Object
}

type hub struct{}

func (hub) Resolve(context.Context, charmhub.Request) (*charmhub.Charm, error) {
	return nil, &charmhub.Error{Code: charmhub.CodeNameNotFound}
}
func (hub) Info(context.Context, string) (*charmhub.Info, error) {
	return &charmhub.Info{Name: "x", Default: &charmhub.ChannelRelease{Channel: "latest/stable", Revision: 3},
		Channels: []charmhub.ChannelRelease{{Channel: "latest/stable", Revision: 3, Version: "1.0", Base: charmhub.Base{Name: "ubuntu", Channel: "22.04", Architecture: "amd64"}}}}, nil
}
func (hub) Find(context.Context, string) ([]charmhub.FindResult, error) {
	return []charmhub.FindResult{{Name: "traefik-k8s", Summary: "Ingress\nmore", Publisher: "Canonical", Version: "2.11"}}, nil
}

func newEnv(t *testing.T, objs ...client.Object) *fakeEnv {
	t.Helper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "m", Labels: map[string]string{v1alpha1.ModelLabel: "true"}}}
	kube := fake.NewClientBuilder().WithScheme(sdk.Scheme()).
		WithStatusSubresource(&v1alpha1.Application{}, &v1alpha1.Relation{}, &v1alpha1.Action{}, &v1alpha1.Offer{}).
		WithObjects(append([]client.Object{ns}, objs...)...).Build()
	return &fakeEnv{t: t, kube: kube, hub: &hub{}}
}

// run executes a command line and returns stdout, stderr and the error.
func (e *fakeEnv) run(args ...string) (string, string, error) {
	e.t.Helper()
	flags := genericclioptions.NewConfigFlags(true)
	c := &cli{flags: flags, stdin: strings.NewReader(e.in), newSDK: func(_ context.Context, ns string) (*sdk.Client, error) {
		return &sdk.Client{Kube: e.kube, Namespace: ns, Hub: e.hub, PollInterval: 2 * time.Millisecond}, nil
	}}
	c.newClient = func() (client.Client, error) {
		scheme := sdk.Scheme()
		_ = apiextensionsv1.AddToScheme(scheme)
		return fake.NewClientBuilder().WithScheme(scheme).Build(), nil
	}
	c.runKubectl = func(a []string) error { e.kubectl = append(e.kubectl, a); return nil }
	c.newKube = func() (kubernetes.Interface, error) { return kubefake.NewSimpleClientset(e.pods...), nil }
	root := newRootWith(c)
	var out, errOut bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs(append([]string{"-m", "m"}, args...))
	err := root.Execute()
	return out.String(), errOut.String(), err
}

func app(name string, scale int32) *v1alpha1.Application {
	return &v1alpha1.Application{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "m"},
		Spec:       v1alpha1.ApplicationSpec{Charm: v1alpha1.CharmSpec{Name: name, Source: "charmhub"}, Scale: &scale},
		Status: v1alpha1.ApplicationStatus{Charm: &v1alpha1.ResolvedCharm{
			Revision: 5, Channel: "latest/stable", Base: "ubuntu@22.04",
			Metadata:     &v1alpha1.JSON{Raw: []byte(`{"name":"` + name + `"}`)},
			ConfigSchema: &v1alpha1.JSON{Raw: []byte(`{"options":{"profile":{"type":"string","default":"production"},"n":{"type":"int","default":1}}}`)},
			Actions:      &v1alpha1.JSON{Raw: []byte(`{"get-password":{"description":"Get a password\nsecond line"}}`)},
		}},
	}
}

func TestAppCommands(t *testing.T) {
	e := newEnv(t, app("pg", 1))
	get := func() *v1alpha1.Application {
		var a v1alpha1.Application
		if err := e.kube.Get(context.Background(), client.ObjectKey{Namespace: "m", Name: "pg"}, &a); err != nil {
			t.Fatal(err)
		}
		return &a
	}
	out, _, err := e.run("scale-application", "pg", "3")
	if err != nil || out != "pg scaled to 3 units\n" || *get().Spec.Scale != 3 {
		t.Fatalf("%q %v", out, err)
	}
	out, _, err = e.run("add-unit", "pg", "-n", "2")
	if err != nil || out != "pg scaled to 5 units\n" {
		t.Fatalf("%q %v", out, err)
	}
	out, _, err = e.run("remove-unit", "pg", "--num-units", "4")
	if err != nil || out != "pg scaled to 1 units\n" {
		t.Fatalf("%q %v", out, err)
	}
	_, _, err = e.run("remove-unit", "pg/0")
	if err == nil || !strings.Contains(err.Error(), "by count") {
		t.Fatalf("%v", err)
	}
	_, _, err = e.run("scale-application", "pg", "many")
	if err == nil || !strings.Contains(err.Error(), "invalid number of units") {
		t.Fatalf("%v", err)
	}

	// config: set, get one, get all, reset.
	if _, _, err = e.run("config", "pg", "profile=testing", "n=4"); err != nil {
		t.Fatal(err)
	}
	if got := get().Spec.Config; got["profile"] != "testing" || got["n"] != "4" {
		t.Fatalf("%v", got)
	}
	out, _, err = e.run("config", "pg", "profile")
	if err != nil || out != "testing\n" {
		t.Fatalf("%q %v", out, err)
	}
	out, _, err = e.run("config", "pg")
	if err != nil || !strings.Contains(out, "application: pg") || !strings.Contains(out, "source: user") {
		t.Fatalf("%q %v", out, err)
	}
	if _, _, err = e.run("config", "pg", "--reset", "profile"); err != nil {
		t.Fatal(err)
	}
	if got := get().Spec.Config; len(got) != 1 {
		t.Fatalf("%v", got)
	}
	if _, _, err = e.run("config", "pg", "nope=1"); err == nil || !strings.Contains(err.Error(), "unknown option") {
		t.Fatalf("%v", err)
	}
	if _, _, err = e.run("config", "pg", "nope"); err == nil {
		t.Fatal("unknown key shown")
	}

	// trust: plain --trust is cluster scope, as in juju.
	if _, _, err = e.run("trust", "pg"); err != nil || get().Spec.Trust != v1alpha1.TrustCluster {
		t.Fatalf("%v %v", err, get().Spec.Trust)
	}
	if _, _, err = e.run("trust", "pg", "--scope", "namespace"); err != nil || get().Spec.Trust != v1alpha1.TrustNamespace {
		t.Fatalf("%v %v", err, get().Spec.Trust)
	}
	if _, _, err = e.run("trust", "pg", "--remove"); err != nil || get().Spec.Trust != v1alpha1.TrustNone {
		t.Fatalf("%v %v", err, get().Spec.Trust)
	}

	out, _, err = e.run("actions", "pg")
	if err != nil || !strings.Contains(out, "get-password") || !strings.Contains(out, "Get a password") || strings.Contains(out, "second line") {
		t.Fatalf("%q %v", out, err)
	}

	out, _, err = e.run("remove-application", "pg", "--no-wait")
	if err != nil || out != "will remove application pg\n" {
		t.Fatalf("%q %v", out, err)
	}
	if _, _, err = e.run("remove-application", "pg"); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("%v", err)
	}
}

func TestTrustFor(t *testing.T) {
	for _, tc := range []struct {
		trust bool
		scope string
		want  v1alpha1.Trust
		err   bool
	}{
		{false, "", v1alpha1.TrustNone, false},
		{true, "", v1alpha1.TrustCluster, false},
		{true, "cluster", v1alpha1.TrustCluster, false},
		{true, "namespace", v1alpha1.TrustNamespace, false},
		{true, "galaxy", "", true},
		{false, "namespace", "", true},
	} {
		got, err := trustFor(tc.trust, tc.scope)
		if (err != nil) != tc.err || got != tc.want {
			t.Errorf("%+v: %v %v", tc, got, err)
		}
	}
}

func TestModelCommands(t *testing.T) {
	e := newEnv(t, app("pg", 1))
	out, _, err := e.run("models")
	if err != nil || !strings.Contains(out, "m*") || !strings.Contains(out, "available") {
		t.Fatalf("%q %v", out, err)
	}
	out, _, err = e.run("models", "--format", "json")
	if err != nil || !strings.Contains(out, `"current-model": "m"`) {
		t.Fatalf("%q %v", out, err)
	}
	if _, _, err = e.run("models", "--format", "toml"); err == nil {
		t.Fatal("bad format accepted")
	}
	if _, _, err = e.run("model-config", "update-status-hook-interval=20s"); err != nil {
		t.Fatal(err)
	}
	out, _, err = e.run("model-config", "update-status-hook-interval")
	if err != nil || out != "20s\n" {
		t.Fatalf("%q %v", out, err)
	}
	out, _, _ = e.run("model-config")
	if !strings.Contains(out, "update-status-hook-interval") {
		t.Fatalf("%q", out)
	}
	if _, _, err = e.run("model-config", "--reset", "update-status-hook-interval"); err != nil {
		t.Fatal(err)
	}
	if out, _, _ = e.run("model-config", "update-status-hook-interval"); out != "5m\n" {
		t.Fatalf("%q", out)
	}
	if _, _, err = e.run("model-config", "nope"); err == nil {
		t.Fatal("unknown key shown")
	}

	// destroy-model asks first, and says no without a terminal or an answer.
	_, _, err = e.run("destroy-model", "m", "--force")
	if err == nil || !strings.Contains(err.Error(), "not destroyed") {
		t.Fatalf("%v", err)
	}
	e.in = "y\n"
	if _, _, err = e.run("destroy-model", "m", "--force"); err != nil {
		t.Fatal(err)
	}
	var ns corev1.Namespace
	if err := e.kube.Get(context.Background(), client.ObjectKey{Name: "m"}, &ns); err == nil {
		t.Fatal("namespace still there")
	}
	if _, _, err = e.run("destroy-model", "m", "--no-prompt", "--destroy-storage", "--release-storage"); err == nil || !strings.Contains(err.Error(), "cannot be used together") {
		t.Fatalf("%v", err)
	}
}

func TestFindAndInfo(t *testing.T) {
	e := newEnv(t)
	out, _, err := e.run("find", "ingress")
	if err != nil || !strings.Contains(out, "traefik-k8s") || strings.Contains(out, "more") || !strings.Contains(out, "Canonical") {
		t.Fatalf("%q %v", out, err)
	}
	out, _, err = e.run("info", "x")
	if err != nil || !strings.Contains(out, "default channel: latest/stable (revision 3)") || !strings.Contains(out, "ubuntu@22.04") {
		t.Fatalf("%q %v", out, err)
	}
}

func TestRunArgs(t *testing.T) {
	units, action, params, err := runArgs([]string{"pg/0", "pg/leader", "get-password", "username=x", "a/b=c"})
	if err != nil || len(units) != 2 || action != "get-password" || len(params) != 2 {
		t.Fatalf("%v %v %v %v", units, action, params, err)
	}
	for _, bad := range [][]string{{"get-password"}, {"pg/0"}, {"a=b/c"}} {
		if _, _, _, err := runArgs(bad); err == nil {
			t.Errorf("%v accepted", bad)
		}
	}
}

func TestStatusOutput(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	st := &sdk.Status{
		Model: sdk.ModelStatus{Name: "chat", Version: "0.1.0", Timestamp: now},
		Applications: map[string]sdk.ApplicationStatus{
			"mattermost-k8s": {Charm: "mattermost-k8s", CharmOrigin: "charmhub", CharmChannel: "latest/stable", CharmRev: 27, Scale: 1, Version: "9.11",
				Status: sdk.StatusInfo{Current: "active"}, Address: "10.152.183.144", Units: map[string]sdk.UnitStatus{
					"mattermost-k8s/0": {Leader: true, WorkloadStatus: sdk.StatusInfo{Current: "active"}, AgentStatus: sdk.StatusInfo{Current: "idle"}, Address: "10.1.32.142", OpenedPorts: []string{"8065/TCP"}},
				}},
			"pg": {Charm: "postgresql-k8s", CharmOrigin: "local", Scale: 11, Status: sdk.StatusInfo{Current: "waiting", Message: "waiting for units"}, Units: map[string]sdk.UnitStatus{
				"pg/10": {WorkloadStatus: sdk.StatusInfo{Current: "waiting", Message: "installing agent"}, AgentStatus: sdk.StatusInfo{Current: "allocating"}},
				"pg/2":  {WorkloadStatus: sdk.StatusInfo{Current: "active", Message: "Primary"}, AgentStatus: sdk.StatusInfo{Current: "idle"}},
			}},
		},
		Relations: []sdk.RelationStatus{{Provider: "pg:database", Requirer: "mattermost-k8s:db", Interface: "pgsql", Type: "regular", Status: "joined"}},
	}
	var b bytes.Buffer
	if err := writeStatus(&b, st, true, false); err != nil {
		t.Fatal(err)
	}
	got := b.String()
	// Colour only adds codes: without them the output is the same, and states get juju's colours.
	var cb bytes.Buffer
	if err := writeStatus(&cb, st, true, true); err != nil {
		t.Fatal(err)
	}
	if plain := ansi.ReplaceAllString(cb.String(), ""); plain != got {
		t.Errorf("colour changed the text:\n%s\nwant\n%s", plain, got)
	}
	for _, want := range []string{green + "active" + reset, yellow + "waiting" + reset, yellow + "allocating" + reset, cyan + "10.152.183.144" + reset, gray + "Primary" + reset} {
		if !strings.Contains(cb.String(), want) {
			t.Errorf("no %q in\n%q", want, cb.String())
		}
	}
	norm := func(s string) string { return strings.Join(strings.Fields(s), " ") }
	for _, want := range []string{
		"Model  Version  Timestamp", "chat   0.1.0    12:00:00Z",
		"App             Version  Status", "mattermost-k8s  9.11     active   1      mattermost-k8s  latest/stable  27   10.152.183.144  no",
		"Unit              Workload", "mattermost-k8s/0*  active    idle   10.1.32.142  8065/TCP",
		"pg/2   active", "pg/10  waiting",
		"Integration provider  Requirer", "pg:database           mattermost-k8s:db  pgsql      regular",
	} {
		found := false
		for _, line := range strings.Split(got, "\n") {
			found = found || strings.Contains(norm(line), norm(want))
		}
		if !found {
			t.Errorf("status lacks %q:\n%s", want, got)
		}
	}
	for _, line := range strings.Split(got, "\n") {
		if line != strings.TrimRight(line, " ") {
			t.Errorf("trailing spaces: %q", line)
		}
	}
	// Local charms show no channel and revision "-"; unit 2 sorts before unit 10.
	if !strings.Contains(got, "postgresql-k8s    -") && !strings.Contains(got, "postgresql-k8s  ") {
		t.Errorf("local charm row:\n%s", got)
	}
	if strings.Index(got, "pg/2 ") > strings.Index(got, "pg/10") {
		t.Errorf("unit order:\n%s", got)
	}
	// Without --relations the integrations table is left out.
	b.Reset()
	_ = writeStatus(&b, st, false, false)
	if strings.Contains(b.String(), "Integration") {
		t.Error("relations shown without the flag")
	}
	b.Reset()
	_ = writeStatus(&b, &sdk.Status{Model: st.Model}, false, false)
	if !strings.Contains(b.String(), "Model is empty.") {
		t.Errorf("%s", b.String())
	}
}

func TestStatusCommandJSON(t *testing.T) {
	e := newEnv(t, app("pg", 1))
	out, _, err := e.run("status", "--format", "json")
	if err != nil || !strings.Contains(out, `"applications"`) || !strings.Contains(out, `"pg"`) || !strings.Contains(out, `"charm-rev": 5`) {
		t.Fatalf("%q %v", out, err)
	}
	out, _, err = e.run("status", "--relations")
	if err != nil || !strings.Contains(out, "pg") || !strings.Contains(out, "waiting") {
		t.Fatalf("%q %v", out, err)
	}
	e2 := newEnv(t)
	out, _, err = e2.run("status")
	if err != nil || !strings.Contains(out, "Model is empty.") {
		t.Fatalf("%q %v", out, err)
	}
}

func TestOfferCommands(t *testing.T) {
	offer := &v1alpha1.Offer{ObjectMeta: metav1.ObjectMeta{Namespace: "m", Name: "pg-offer"},
		Spec: v1alpha1.OfferSpec{Application: "pg", Endpoints: []string{"database"}, AllowedModels: []string{"web", "cos"}},
		Status: v1alpha1.OfferStatus{Endpoints: []v1alpha1.OfferEndpoint{{Name: "database", Interface: "pgsql", Role: "provider"}},
			Connections: []v1alpha1.OfferConnection{{Namespace: "web", Relation: "r", Application: "client", Endpoint: "db", Status: "joined"}}}}
	idle := &v1alpha1.Offer{ObjectMeta: metav1.ObjectMeta{Namespace: "m", Name: "idle"}, Spec: v1alpha1.OfferSpec{Application: "pg", Endpoints: []string{"database", "extra"}}}
	e := newEnv(t, offer, idle)

	out, _, err := e.run("offers")
	if err != nil || !strings.Contains(out, "m.pg-offer") || !strings.Contains(out, "web,cos") || !strings.Contains(out, "database,extra") {
		t.Fatalf("%q %v", out, err)
	}
	out, _, err = e.run("offers", "--format", "json")
	if err != nil || !strings.Contains(out, `"allowedModels"`) {
		t.Fatalf("%q %v", out, err)
	}
	out, _, err = e.run("show-offer", "m.pg-offer")
	if err != nil || !strings.Contains(out, "url: m.pg-offer") || !strings.Contains(out, "interface: pgsql") || !strings.Contains(out, "relation: r") {
		t.Fatalf("%q %v", out, err)
	}
	if _, _, err = e.run("show-offer", "pg-offer"); err != nil { // an offer of the current model by its bare name
		t.Fatal(err)
	}
	if _, _, err = e.run("show-offer", "m.nope"); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("%v", err)
	}

	// An offer with connections is only removed with --force.
	if _, _, err = e.run("remove-offer", "pg-offer"); err == nil || !strings.Contains(err.Error(), "--force") {
		t.Fatalf("%v", err)
	}
	out, _, err = e.run("remove-offer", "idle")
	if err != nil || out != "Removed offer idle\n" {
		t.Fatalf("%q %v", out, err)
	}
	if _, _, err = e.run("remove-offer", "pg-offer", "--force"); err != nil {
		t.Fatal(err)
	}

	// Malformed arguments are refused before the cluster is asked.
	for _, bad := range [][]string{{"offer", "pg"}, {"offer", "pg:"}, {"offer", ":database"}} {
		if _, _, err := e.run(bad...); err == nil || !strings.Contains(err.Error(), "invalid offer") {
			t.Errorf("%v: %v", bad, err)
		}
	}
	for name, want := range map[string]string{"consume": "integrate", "remove-saas": "remove-relation"} {
		if _, _, err := e.run(name); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestIntegrateFlags(t *testing.T) {
	e := newEnv(t, app("web", 1))
	if _, _, err := e.run("integrate", "web", "web2", "--alias", "x"); err == nil || !strings.Contains(err.Error(), "--alias applies to offers") {
		t.Fatalf("%v", err)
	}
	if _, _, err := e.run("integrate", "web", "far.pg", "--alias", "x"); err == nil || !strings.Contains(err.Error(), `offer "pg" not found in model "far"`) {
		t.Fatalf("%v", err)
	}
}

var ansi = regexp.MustCompile("\x1b\\[[0-9;]*m")

func TestStatusFilterAndColourFlags(t *testing.T) {
	e := newEnv(t, app("pg", 2), app("web", 1))
	out, _, err := e.run("status", "pg*")
	if err != nil || !strings.Contains(out, "pg/0") || strings.Contains(out, "web") {
		t.Fatalf("%q %v", out, err)
	}
	// A unit shows alone, and its application still has its row.
	out, _, err = e.run("status", "pg/1")
	if err != nil || strings.Contains(out, "pg/0") || !strings.Contains(out, "pg/1") {
		t.Fatalf("%q %v", out, err)
	}
	if _, _, err = e.run("status", "nope"); err == nil || !strings.Contains(err.Error(), "nothing matches nope") {
		t.Fatalf("%v", err)
	}
	if _, _, err = e.run("status", "[a"); err == nil || !strings.Contains(err.Error(), "invalid pattern") {
		t.Fatalf("%v", err)
	}
	out, _, err = e.run("status", "--color")
	if err != nil || !ansi.MatchString(out) {
		t.Fatalf("--color gave no colour: %q %v", out, err)
	}
	out, _, err = e.run("status", "--no-color")
	if err != nil || ansi.MatchString(out) {
		t.Fatalf("--no-color gave colour: %q %v", out, err)
	}
	if _, _, err = e.run("status", "--color", "--no-color"); err == nil {
		t.Fatal("--color with --no-color accepted")
	}
	if _, _, err = e.run("status", "--integrations"); err != nil {
		t.Fatal(err)
	}
}

func TestJujuAliasesAndSmartFormat(t *testing.T) {
	root := newRoot()
	for alias, name := range map[string]string{"list-models": "models", "list-actions": "actions", "list-operations": "operations",
		"list-offers": "offers", "resolve": "resolved", "relate": "integrate"} {
		if c, _, err := root.Find([]string{alias}); err != nil || c.Name() != name {
			t.Errorf("%s is not an alias of %s (%v)", alias, name, err)
		}
	}
	e := newEnv(t, app("pg", 1))
	smart, _, err := e.run("status", "--format", "smart")
	plain, _, err2 := e.run("status")
	if err != nil || err2 != nil || smart != plain || !strings.Contains(smart, "Workload") {
		t.Fatalf("smart differs from tabular: %v %v\n%s\n%s", err, err2, smart, plain)
	}
}
