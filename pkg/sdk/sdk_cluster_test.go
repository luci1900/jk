package sdk

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/internal/agent"
	"github.com/luci1900/jk/internal/operator"
	"github.com/luci1900/jk/pkg/charmhub"
)

func TestModels(t *testing.T) {
	c, _ := newClient(t, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "plain"}})
	ctx := context.Background()
	noErr(t, c.AddModel(ctx, "chat"))
	wantErr(t, c.AddModel(ctx, "chat"), `model "chat" already exists`)
	wantErr(t, c.AddModel(ctx, "plain"), `model "plain" already exists`)
	wantErr(t, c.AddModel(ctx, ""), "model name is required")
	ms, err := c.Models(ctx)
	noErr(t, err)
	if len(ms) != 2 || ms[0].Name != "chat" || ms[1].Name != "m" {
		t.Fatalf("%+v", ms)
	}
	// The model's id counters and config exist right away.
	var cm corev1.ConfigMap
	noErr(t, c.Kube.Get(ctx, client.ObjectKey{Namespace: "chat", Name: v1alpha1.ModelConfigMap}, &cm))
	if cm.Data["relation-id"] != "0" {
		t.Fatalf("%v", cm.Data)
	}
}

func TestDestroyModel(t *testing.T) {
	ctx := context.Background()
	t.Run("not a jk model", func(t *testing.T) {
		c, _ := newClient(t, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "kube-system"}})
		wantErr(t, c.DestroyModel(ctx, "kube-system", DestroyOptions{Force: true}), "not a jk model")
		wantErr(t, c.DestroyModel(ctx, "nope", DestroyOptions{}), `model "nope" not found`)
		var ns corev1.Namespace
		noErr(t, c.Kube.Get(ctx, client.ObjectKey{Name: "kube-system"}, &ns))
	})
	t.Run("force marks storage and deletes the namespace", func(t *testing.T) {
		c, _ := newClient(t, resolved("db", "db", dbMetadata, "", "", 1))
		// A fake namespace delete is immediate, so look at the annotation through a recording client.
		rec := &recorder{Client: c.Kube}
		c.Kube = rec
		noErr(t, c.DestroyModel(ctx, "m", DestroyOptions{Force: true, DestroyStorage: true}))
		if rec.annotated["db"] != "true" {
			t.Fatalf("destroy-storage annotation missing: %v", rec.annotated)
		}
		var ns corev1.Namespace
		if err := c.Kube.Get(ctx, client.ObjectKey{Name: "m"}, &ns); !apierrors.IsNotFound(err) {
			t.Fatalf("namespace still there: %v", err)
		}
	})
	t.Run("graceful removes applications first", func(t *testing.T) {
		c, _ := newClient(t, resolved("db", "db", dbMetadata, "", "", 1), resolved("web", "web", webMetadata, "", "", 1))
		noErr(t, c.DestroyModel(ctx, "m", DestroyOptions{}))
		var apps v1alpha1.ApplicationList
		noErr(t, c.Kube.List(ctx, &apps, client.InNamespace("m")))
		if len(apps.Items) != 0 {
			t.Fatalf("applications left: %d", len(apps.Items))
		}
	})
	t.Run("graceful gives up when applications stay", func(t *testing.T) {
		app := resolved("db", "db", dbMetadata, "", "", 1)
		app.Finalizers = []string{"jk.luci1900.github.io/application"}
		c, _ := newClient(t, app)
		err := c.DestroyModel(ctx, "m", DestroyOptions{Timeout: 50 * time.Millisecond})
		wantErr(t, err, "still removing db")
		wantErr(t, err, "--force")
		var ns corev1.Namespace
		noErr(t, c.Kube.Get(ctx, client.ObjectKey{Name: "m"}, &ns))
	})
}

// recorder notes the destroy-storage annotation of Applications as they are updated.
type recorder struct {
	client.Client
	annotated map[string]string
}

func (r *recorder) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if a, ok := obj.(*v1alpha1.Application); ok {
		if r.annotated == nil {
			r.annotated = map[string]string{}
		}
		r.annotated[a.Name] = a.Annotations[v1alpha1.DestroyStorageAnnotation]
	}
	return r.Client.Update(ctx, obj, opts...)
}

func TestDestroyModelWaitsForTheNamespaceToGo(t *testing.T) {
	ctx := context.Background()
	stuck := model("stuck")
	stuck.Finalizers = []string{"kubernetes"}
	c, _ := newClient(t, stuck)
	err := c.DestroyModel(ctx, "stuck", DestroyOptions{Force: true, Timeout: 40 * time.Millisecond})
	wantErr(t, err, `timed out waiting for model "stuck" to be removed`)
	noErr(t, c.DestroyModel(ctx, "stuck", DestroyOptions{Force: true, NoWait: true}))
}

func TestModelConfig(t *testing.T) {
	ctx := context.Background()
	c, _ := newClient(t)
	noErr(t, c.SetModelConfig(ctx, map[string]string{"update-status-hook-interval": "30s", "logging-config": "x"}))
	got, err := c.ModelConfig(ctx)
	noErr(t, err)
	if got["update-status-hook-interval"] != "30s" || got["logging-config"] != "x" || len(got) != 2 {
		t.Fatalf("%v", got)
	}
	// Resetting returns a key with a default to it and removes the others.
	noErr(t, c.SetModelConfig(ctx, map[string]string{"update-status-hook-interval": "", "logging-config": ""}))
	got, _ = c.ModelConfig(ctx)
	if got["update-status-hook-interval"] != "5m" || len(got) != 1 {
		t.Fatalf("%v", got)
	}
	// The id counters are not model config.
	if _, ok := got["relation-id"]; ok {
		t.Fatal("counter shown as model config")
	}
}

// resolveCharmsLoop plays the operator's part for new Applications: it resolves their charm into status.
func resolveCharmsLoop(c *Client, hub *fakeHub) func(ctx context.Context, c *Client) {
	return func(ctx context.Context, c *Client) {
		var apps v1alpha1.ApplicationList
		if err := c.Kube.List(ctx, &apps, client.InNamespace(c.Namespace)); err != nil {
			return
		}
		for i := range apps.Items {
			a := &apps.Items[i]
			if a.Status.Charm != nil && a.Status.Charm.Pin == pinOf(a) {
				continue
			}
			ch := hub.charms[a.Spec.Charm.Name]
			if ch == nil {
				continue
			}
			md, _ := yamlToJSON(ch.MetadataYAML)
			cf, _ := yamlToJSON(ch.ConfigYAML)
			ac, _ := yamlToJSON(ch.ActionsYAML)
			rev := ch.Revision
			if a.Spec.Charm.Revision != nil {
				rev = *a.Spec.Charm.Revision
			}
			channel := "latest/stable"
			if a.Spec.Charm.Revision != nil {
				channel = "" // as the operator does: a revision pin leaves no channel in status
			}
			a.Status.Charm = &v1alpha1.ResolvedCharm{Revision: rev, Channel: channel, Base: "ubuntu@22.04", Image: "reg/charms@sha256:abc", Metadata: md, ConfigSchema: cf, Actions: ac, Pin: pinOf(a)}
			_ = c.Kube.Status().Update(ctx, a)
		}
	}
}

func pinOf(a *v1alpha1.Application) string { return operator.PinKey(a) }

func withOperator(t *testing.T, c *Client, step func(ctx context.Context, c *Client)) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var sim operatorSim
	sim.run(ctx, c, step)
	t.Cleanup(func() { sim.stop(cancel) })
}

func TestDeployCharmhub(t *testing.T) {
	ctx := context.Background()
	c, hub := newClient(t)
	withOperator(t, c, resolveCharmsLoop(c, hub))
	scale := 2
	res, err := c.Deploy(ctx, DeployOptions{
		Charm: "db", Name: "pg", Channel: "14/stable", Scale: scale, Trust: v1alpha1.TrustCluster,
		Config: map[string]string{"profile": "testing"}, Storage: map[string]v1alpha1.StorageSpec{"data": {Size: "1G"}},
		Resources: map[string]string{"db-image": "img:1"},
	})
	noErr(t, err)
	// The channel is the one Charmhub found the revision in.
	if res.String() != `Deployed "pg" from charm-hub charm "db", revision 7 in channel 14/stable on ubuntu@22.04` {
		t.Fatal(res.String())
	}
	a := mustGet(t, c, "pg")
	if a.Spec.Charm.Name != "db" || a.Spec.Charm.Channel != "14/stable" || *a.Spec.Scale != 2 || a.Spec.Trust != v1alpha1.TrustCluster || a.Spec.Config["profile"] != "testing" {
		t.Fatalf("%+v", a.Spec)
	}
	// The revision and base are pinned, so the deploy can be repeated; the channel stays what the user asked for.
	if got := a.Spec.Charm; got.Revision == nil || *got.Revision != 7 || got.Base != "ubuntu@22.04" || got.Channel != "14/stable" {
		t.Fatalf("pins: %+v", got)
	}
	// The cluster's architecture was passed to Charmhub for the early check.
	if hub.last.Base.Architecture != "amd64" || hub.last.Name != "db" {
		t.Fatalf("%+v", hub.last)
	}
}

func TestDeployChecksBeforeCreating(t *testing.T) {
	ctx := context.Background()
	c, hub := newClient(t)
	cases := []struct {
		name string
		o    DeployOptions
		want string
	}{
		{"unknown option", DeployOptions{Charm: "db", Config: map[string]string{"nope": "1"}}, `unknown option "nope"`},
		{"bad type", DeployOptions{Charm: "db", Config: map[string]string{"connections": "x"}}, "connections"},
		{"unknown resource", DeployOptions{Charm: "db", Resources: map[string]string{"x": "y"}}, `unknown resource "x"`},
		{"unknown storage", DeployOptions{Charm: "db", Storage: map[string]v1alpha1.StorageSpec{"x": {}}}, `no storage "x"`},
		{"bad name", DeployOptions{Charm: "db", Name: "DB"}, "invalid application name"},
		{"no such charm", DeployOptions{Charm: "nothing"}, `charm "nothing" not found in Charmhub`},
		{"local with channel", DeployOptions{Charm: "./x.charm", Channel: "edge"}, "apply to Charmhub charms"},
		{"directory", DeployOptions{Charm: t.TempDir() + "/"}, "is a directory"},
		{"no charm", DeployOptions{}, "no charm specified"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := c.Deploy(ctx, tc.o)
			wantErr(t, err, tc.want)
		})
	}
	var apps v1alpha1.ApplicationList
	noErr(t, c.Kube.List(ctx, &apps))
	if len(apps.Items) != 0 {
		t.Fatalf("an Application was created by a rejected deploy: %v", apps.Items)
	}
	// An application that exists is reported as juju does.
	c2, hub2 := newClient(t, resolved("db", "db", dbMetadata, "", "", 1))
	_ = hub2
	_, err := c2.Deploy(ctx, DeployOptions{Charm: "db"})
	wantErr(t, err, `application "db" already exists`)
	// Charmhub problems that are not "not found" come through.
	hub.err = &charmhub.Error{Code: "boom", Message: "store down"}
	_, err = c.Deploy(ctx, DeployOptions{Charm: "db"})
	wantErr(t, err, "store down")
	// A directory of a charm and a missing file are explained.
	_, err = c.Deploy(ctx, DeployOptions{Charm: filepath.Join(t.TempDir(), "x.charm")})
	wantErr(t, err, "cannot use charm")
}

func TestDeployWithoutClusterArchSkipsEarlyChecks(t *testing.T) {
	ctx := context.Background()
	c, hub := newClient(t)
	// Two architectures: the operator will complain, the SDK must not guess.
	noErr(t, c.Kube.Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n2"}, Status: corev1.NodeStatus{NodeInfo: corev1.NodeSystemInfo{Architecture: "arm64"}}}))
	withOperator(t, c, resolveCharmsLoop(c, hub))
	_, err := c.Deploy(ctx, DeployOptions{Charm: "db", Config: map[string]string{"nope": "1"}})
	noErr(t, err)
	if hub.last.Name != "" {
		t.Fatalf("Charmhub asked without an architecture: %+v", hub.last)
	}
}

func TestDeployReportsCharmNotFoundFromOperator(t *testing.T) {
	ctx := context.Background()
	c, _ := newClient(t)
	noErr(t, c.Kube.Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n2"}, Status: corev1.NodeStatus{NodeInfo: corev1.NodeSystemInfo{Architecture: "arm64"}}}))
	withOperator(t, c, func(ctx context.Context, c *Client) {
		var apps v1alpha1.ApplicationList
		_ = c.Kube.List(ctx, &apps, client.InNamespace(c.Namespace))
		for i := range apps.Items {
			meta.SetStatusCondition(&apps.Items[i].Status.Conditions, metav1.Condition{Type: "CharmUpToDate", Status: metav1.ConditionFalse, Reason: "CharmNotFound", Message: "no such revision"})
			_ = c.Kube.Status().Update(ctx, &apps.Items[i])
		}
	})
	_, err := c.Deploy(ctx, DeployOptions{Charm: "db"})
	wantErr(t, err, "no such revision")
}

func TestConfig(t *testing.T) {
	ctx := context.Background()
	c, _ := newClient(t, resolved("pg", "db", dbMetadata, dbConfig, dbActions, 1))
	v, err := c.Config(ctx, "pg")
	noErr(t, err)
	if v.Settings["profile"].Value != "production" || v.Settings["profile"].Source != "default" || v.Settings["connections"].Value != int64(100) {
		t.Fatalf("%+v", v.Settings)
	}
	noErr(t, c.SetConfig(ctx, "pg", map[string]string{"profile": "testing", "connections": "7"}))
	v, _ = c.Config(ctx, "pg")
	if v.Settings["profile"].Value != "testing" || v.Settings["profile"].Source != "user" || v.Settings["connections"].Value != int64(7) {
		t.Fatalf("%+v", v.Settings)
	}
	wantErr(t, c.SetConfig(ctx, "pg", map[string]string{"nope": "1"}), "unknown option")
	wantErr(t, c.SetConfig(ctx, "pg", map[string]string{"connections": "lots"}), "connections")
	if got := mustGet(t, c, "pg").Spec.Config; len(got) != 2 {
		t.Fatalf("a rejected set changed the config: %v", got)
	}
	noErr(t, c.ResetConfig(ctx, "pg", []string{"profile"}))
	if got := mustGet(t, c, "pg").Spec.Config; len(got) != 1 || got["connections"] != "7" {
		t.Fatalf("%v", got)
	}
	wantErr(t, c.ResetConfig(ctx, "pg", []string{"nope"}), "unknown option")
	noErr(t, c.ResetConfig(ctx, "pg", []string{"connections"}))
	if got := mustGet(t, c, "pg").Spec.Config; got != nil {
		t.Fatalf("%v", got)
	}
	_, err = c.Config(ctx, "missing")
	wantErr(t, err, `application "missing" not found`)
}

func TestScaleTrustRemove(t *testing.T) {
	ctx := context.Background()
	c, _ := newClient(t, resolved("pg", "db", dbMetadata, dbConfig, "", 2))
	n, err := c.AddUnits(ctx, "pg", 2)
	noErr(t, err)
	if n != 4 || *mustGet(t, c, "pg").Spec.Scale != 4 {
		t.Fatalf("scale %d", n)
	}
	n, err = c.RemoveUnits(ctx, "pg", 3)
	noErr(t, err)
	if n != 1 {
		t.Fatalf("scale %d", n)
	}
	_, err = c.RemoveUnits(ctx, "pg", 2)
	wantErr(t, err, "cannot remove 2 units: application \"pg\" has 1")
	_, err = c.AddUnits(ctx, "pg", 0)
	wantErr(t, err, "invalid number of units")
	noErr(t, c.Scale(ctx, "pg", 0))
	wantErr(t, c.Scale(ctx, "pg", -1), "invalid number of units")
	noErr(t, c.SetTrust(ctx, "pg", v1alpha1.TrustCluster))
	if mustGet(t, c, "pg").Spec.Trust != v1alpha1.TrustCluster {
		t.Fatal("trust not set")
	}
	wantErr(t, c.SetTrust(ctx, "pg", "everything"), "invalid trust")
	wantErr(t, c.Scale(ctx, "missing", 1), "not found")

	rec := &recorder{Client: c.Kube}
	c.Kube = rec
	noErr(t, c.RemoveApplication(ctx, "pg", true))
	if rec.annotated["pg"] != "true" {
		t.Fatalf("%v", rec.annotated)
	}
	wantErr(t, c.RemoveApplication(ctx, "pg", false), "not found")
	noErr(t, c.WaitRemoved(ctx, "pg", time.Second))
}

func TestRefresh(t *testing.T) {
	ctx := context.Background()
	c, hub := newClient(t, resolved("pg", "db", dbMetadata, dbConfig, "", 1))
	hub.charms["db"].Revision = 9
	withOperator(t, c, resolveCharmsLoop(c, hub))
	// Mark the existing status as resolved from the current pins.
	a := mustGet(t, c, "pg")
	a.Status.Charm.Pin = pinOf(a)
	noErr(t, c.Kube.Status().Update(ctx, a))

	// No arguments: the newest revision of the channel, pinned by number.
	res, err := c.Refresh(ctx, "pg", RefreshOptions{})
	noErr(t, err)
	if res.Revision != 9 || res.String() != `Added charm-hub charm "db", revision 9 in channel latest/stable, to the model` {
		t.Fatalf("%+v %s", res, res)
	}
	if rev := mustGet(t, c, "pg").Spec.Charm.Revision; rev == nil || *rev != 9 {
		t.Fatalf("revision pin %v", rev)
	}
	// A channel switch drops the revision pin.
	res, err = c.Refresh(ctx, "pg", RefreshOptions{Channel: "edge"})
	noErr(t, err)
	if got := mustGet(t, c, "pg").Spec.Charm; got.Channel != "edge" || got.Revision != nil {
		t.Fatalf("%+v", got)
	}
	r3 := 3
	_, err = c.Refresh(ctx, "pg", RefreshOptions{Revision: &r3})
	noErr(t, err)
	if rev := mustGet(t, c, "pg").Spec.Charm.Revision; *rev != 3 {
		t.Fatalf("revision pin %v", *rev)
	}
	_, err = c.Refresh(ctx, "pg", RefreshOptions{Path: "x.charm", Channel: "edge"})
	wantErr(t, err, "cannot be combined")
	_, err = c.Refresh(ctx, "nope", RefreshOptions{})
	wantErr(t, err, "not found")

	// Local charms refresh only from a file.
	local := resolved("loc", "db", dbMetadata, "", "", 1)
	local.Spec.Charm.Source = "local"
	c2, _ := newClient(t, local)
	_, err = c2.Refresh(ctx, "loc", RefreshOptions{})
	wantErr(t, err, "refresh it with --path")
}

func TestRefreshReportsFailure(t *testing.T) {
	ctx := context.Background()
	c, _ := newClient(t, resolved("pg", "db", dbMetadata, dbConfig, "", 1))
	a := mustGet(t, c, "pg")
	a.Status.Charm.Pin = "old"
	noErr(t, c.Kube.Status().Update(ctx, a))
	withOperator(t, c, func(ctx context.Context, c *Client) {
		var cur v1alpha1.Application
		if err := c.Kube.Get(ctx, client.ObjectKey{Namespace: "m", Name: "pg"}, &cur); err != nil {
			return
		}
		meta.SetStatusCondition(&cur.Status.Conditions, metav1.Condition{Type: "CharmUpToDate", Status: metav1.ConditionFalse, Reason: "CharmNotFound", Message: "revision 99 not found", ObservedGeneration: cur.Generation})
		_ = c.Kube.Status().Update(ctx, &cur)
	})
	r := 99
	_, err := c.Refresh(ctx, "pg", RefreshOptions{Revision: &r})
	wantErr(t, err, "revision 99 not found")
}

func TestResolved(t *testing.T) {
	ctx := context.Background()
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pg-0", Namespace: "m"}}
	c, _ := newClient(t, pod)
	noErr(t, c.Resolved(ctx, "pg/0", false))
	var got corev1.Pod
	noErr(t, c.Kube.Get(ctx, client.ObjectKey{Namespace: "m", Name: "pg-0"}, &got))
	if got.Annotations[agent.ResolvedAnnotation] != "retry" {
		t.Fatalf("%v", got.Annotations)
	}
	noErr(t, c.Resolved(ctx, "pg/0", true))
	noErr(t, c.Kube.Get(ctx, client.ObjectKey{Namespace: "m", Name: "pg-0"}, &got))
	if got.Annotations[agent.ResolvedAnnotation] != "no-retry" {
		t.Fatalf("%v", got.Annotations)
	}
	wantErr(t, c.Resolved(ctx, "pg/9", false), `unit "pg/9" not found`)
	wantErr(t, c.Resolved(ctx, "pg", false), "invalid unit")
}
