// Package sdk is the client side of jk: it turns juju-style operations (deploy, integrate, run, status ...) into the
// objects the operator and the unit agents act on, and checks requests early with juju-style messages. The jk CLI is
// a thin layer on top. What must hold for every client is also enforced by the operator and the CRD schemas.
package sdk

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/internal/registry"
	"github.com/luci1900/jk/pkg/charmhub"
)

// Hub is what the SDK needs from Charmhub; *charmhub.Client implements it.
type Hub interface {
	Resolve(ctx context.Context, r charmhub.Request) (*charmhub.Charm, error)
	Info(ctx context.Context, name string) (*charmhub.Info, error)
	Find(ctx context.Context, query string) ([]charmhub.FindResult, error)
}

// RegistryOpener connects to jk-registry for pushing local charms; the returned function closes the connection.
type RegistryOpener func(ctx context.Context) (*registry.Client, func(), error)

// Client operates on one model (a namespace).
type Client struct {
	Kube      client.Client
	Namespace string
	Hub       Hub
	// Registry is only needed to deploy or refresh local charms.
	Registry RegistryOpener

	// PollInterval is how often waits look at the cluster (default 300ms).
	PollInterval time.Duration
	// Now is the clock (default time.Now).
	Now func() time.Time
}

// Scheme returns a scheme with the types the SDK reads and writes.
func Scheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	_ = v1alpha1.AddToScheme(s)
	return s
}

// New builds a Client for the namespace from a rest config. Local charms are pushed through a port-forward to jk-registry.
func New(cfg *rest.Config, namespace string) (*Client, error) {
	kube, err := client.New(cfg, client.Options{Scheme: Scheme()})
	if err != nil {
		return nil, err
	}
	return &Client{
		Kube:      kube,
		Namespace: namespace,
		Hub:       &charmhub.Client{UserAgent: "jk-cli"},
		Registry:  func(ctx context.Context) (*registry.Client, func(), error) { return registry.Connect(ctx, cfg) },
	}, nil
}

// InNamespace returns a copy of the Client for another model.
func (c *Client) InNamespace(ns string) *Client {
	cp := *c
	cp.Namespace = ns
	return &cp
}

func (c *Client) poll() time.Duration {
	if c.PollInterval > 0 {
		return c.PollInterval
	}
	return 300 * time.Millisecond
}

func (c *Client) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}

// wait calls cond until it reports done, fails, or the timeout passes (then the last reason is part of the error).
func (c *Client) wait(ctx context.Context, timeout time.Duration, what string, cond func(ctx context.Context) (done bool, reason string, err error)) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	last := ""
	for {
		done, reason, err := cond(ctx)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		last = reason
		select {
		case <-ctx.Done():
			if last != "" {
				return fmt.Errorf("timed out waiting for %s: %s", what, last)
			}
			return fmt.Errorf("timed out waiting for %s", what)
		case <-time.After(c.poll()):
		}
	}
}

// ErrNotJKModel means a namespace is not marked as a jk model.
var ErrNotJKModel = errors.New("not a jk model")

// ErrNotInstalled means the cluster has no jk: its API types are missing.
var ErrNotInstalled = errors.New("jk is not installed in the cluster: run `kubectl jk install`")

// requireInstalled checks that the cluster serves jk's API types. It asks for one Application: a cluster without
// the CRDs fails that before any request is made, and any other error (such as no right to list) means jk is there.
func (c *Client) requireInstalled(ctx context.Context) error {
	var apps v1alpha1.ApplicationList
	err := c.Kube.List(ctx, &apps, client.InNamespace(c.Namespace), client.Limit(1))
	if meta.IsNoMatchError(err) || runtime.IsNotRegisteredError(err) {
		return ErrNotInstalled
	}
	return nil
}

// requireModel checks that the client's namespace is a live jk model.
func (c *Client) requireModel(ctx context.Context) error {
	if err := c.requireInstalled(ctx); err != nil {
		return err
	}
	ns, err := c.namespace(ctx, c.Namespace)
	if err != nil {
		return err
	}
	if ns.DeletionTimestamp != nil {
		return fmt.Errorf("model %q is being destroyed", c.Namespace)
	}
	return nil
}

// Unit names: "app/0" for the unit, "app-0" for its pod and UnitData.
func unitName(app string, n int) string { return fmt.Sprintf("%s/%d", app, n) }

func splitUnit(unit string) (app string, n int, ok bool) {
	i := strings.LastIndex(unit, "/")
	if i <= 0 {
		return "", 0, false
	}
	if _, err := fmt.Sscanf(unit[i+1:], "%d", &n); err != nil || fmt.Sprint(n) != unit[i+1:] {
		return "", 0, false
	}
	return unit[:i], n, true
}

func podName(unit string) string { return strings.ReplaceAll(unit, "/", "-") }

// getApplication reads an Application, with juju's wording when it does not exist.
func (c *Client) getApplication(ctx context.Context, name string) (*v1alpha1.Application, error) {
	var app v1alpha1.Application
	err := c.Kube.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: name}, &app)
	if apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("application %q not found", name)
	}
	if err != nil {
		return nil, err
	}
	return &app, nil
}

// updateApplication applies mutate to the Application and writes it back, retrying on conflicts with the operator's writes.
func (c *Client) updateApplication(ctx context.Context, name string, mutate func(*v1alpha1.Application) error) (*v1alpha1.Application, error) {
	var updated *v1alpha1.Application
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		app, err := c.getApplication(ctx, name)
		if err != nil {
			return err
		}
		if err := mutate(app); err != nil {
			return err
		}
		if err := c.Kube.Update(ctx, app); err != nil {
			return err
		}
		updated = app
		return nil
	})
	return updated, err
}
