package sdk

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/pkg/charmhub"
)

const (
	dbMetadata = `
name: db
summary: a database
provides:
  database: {interface: pgsql}
  metrics: prometheus_scrape
peers:
  cluster: {interface: db_peers}
storage:
  data: {type: filesystem}
resources:
  db-image: {type: oci-image}
`
	dbConfig = `
options:
  profile: {type: string, default: production, description: the profile}
  connections: {type: int, default: 100}
  fsync: {type: boolean, default: true}
`
	dbActions = `
get-password:
  description: Get a password
`
	webMetadata = `
name: web
requires:
  db: {interface: pgsql, limit: 1}
  scrape: {interface: prometheus_scrape}
`
	otherMetadata = `
name: other
requires:
  database: {interface: pgsql}
`
)

type fakeHub struct {
	charms map[string]*charmhub.Charm
	err    error
	last   charmhub.Request
}

func (h *fakeHub) Resolve(_ context.Context, r charmhub.Request) (*charmhub.Charm, error) {
	h.last = r
	if h.err != nil {
		return nil, h.err
	}
	ch, ok := h.charms[r.Name]
	if !ok {
		return nil, &charmhub.Error{Code: charmhub.CodeNameNotFound}
	}
	return ch, nil
}
func (h *fakeHub) Info(context.Context, string) (*charmhub.Info, error) { return &charmhub.Info{}, nil }
func (h *fakeHub) Find(context.Context, string) ([]charmhub.FindResult, error) {
	return []charmhub.FindResult{{Name: "x"}}, nil
}

func hubWith() *fakeHub {
	return &fakeHub{charms: map[string]*charmhub.Charm{
		"db":    {Name: "db", Revision: 7, Channel: "14/stable", Base: charmhub.Base{Name: "ubuntu", Channel: "22.04", Architecture: "amd64"}, MetadataYAML: dbMetadata, ConfigYAML: dbConfig, ActionsYAML: dbActions},
		"web":   {Name: "web", Revision: 3, MetadataYAML: webMetadata},
		"other": {Name: "other", Revision: 1, MetadataYAML: otherMetadata},
	}}
}

func node(arch string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1"}, Status: corev1.NodeStatus{NodeInfo: corev1.NodeSystemInfo{Architecture: arch}}}
}

func model(name string) *corev1.Namespace {
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{v1alpha1.ModelLabel: "true"}}}
}

// newClient returns a Client on a fake cluster with one model "m" and one amd64 node.
func newClient(t *testing.T, objs ...client.Object) (*Client, *fakeHub) {
	t.Helper()
	hub := hubWith()
	kube := fake.NewClientBuilder().WithScheme(Scheme()).
		WithStatusSubresource(&v1alpha1.Application{}, &v1alpha1.Relation{}, &v1alpha1.Action{}, &v1alpha1.Offer{}).
		WithObjects(append([]client.Object{model("m"), node("amd64")}, objs...)...).Build()
	return &Client{Kube: kube, Namespace: "m", Hub: hub, PollInterval: 5 * time.Millisecond}, hub
}

// resolved is an Application with its charm resolved by the operator.
func resolved(name, charmName, md, cfg, actions string, scale int32) *v1alpha1.Application {
	meta, _ := yamlToJSON(md)
	conf, _ := yamlToJSON(cfg)
	act, _ := yamlToJSON(actions)
	return &v1alpha1.Application{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "m"},
		Spec:       v1alpha1.ApplicationSpec{Charm: v1alpha1.CharmSpec{Name: charmName, Source: "charmhub"}, Scale: &scale},
		Status: v1alpha1.ApplicationStatus{Charm: &v1alpha1.ResolvedCharm{
			Revision: 7, Channel: "latest/stable", Base: "ubuntu@22.04", Image: "reg/charms@sha256:abc",
			Metadata: meta, ConfigSchema: conf, Actions: act,
		}},
	}
}

func yamlToJSON(s string) (*v1alpha1.JSON, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	b, err := yamlBytes(s)
	if err != nil {
		return nil, err
	}
	return &v1alpha1.JSON{Raw: b}, nil
}

// operatorSim fills in what the operator would, for the objects the test creates: it runs until the context ends.
type operatorSim struct {
	wg sync.WaitGroup
}

func (o *operatorSim) stop(cancel context.CancelFunc) { cancel(); o.wg.Wait() }

func (o *operatorSim) run(ctx context.Context, c *Client, step func(ctx context.Context, c *Client)) {
	o.wg.Add(1)
	go func() {
		defer o.wg.Done()
		for ctx.Err() == nil {
			step(ctx, c)
			time.Sleep(2 * time.Millisecond)
		}
	}()
}

func mustGet(t *testing.T, c *Client, name string) *v1alpha1.Application {
	t.Helper()
	a, err := c.getApplication(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func wantErr(t *testing.T, err error, substr string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), substr) {
		t.Fatalf("error = %v, want one containing %q", err, substr)
	}
}

func noErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(fmt.Sprintf("unexpected error: %v", err))
	}
}
