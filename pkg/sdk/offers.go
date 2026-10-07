package sdk

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/luci1900/jk/api/v1alpha1"
)

// OfferOptions describe an offer to make.
type OfferOptions struct {
	// Application and Endpoints are what is offered (in the client's model).
	Application string
	Endpoints   []string
	// Name is the offer's name (default: the application's name).
	Name string
	// AllowedModels are the models (namespaces) that may consume the offer, "*" for all. Nil takes the model config
	// `offer-allowed-models` (a comma-separated list; none by default).
	AllowedModels []string
}

// OfferURL is how an offer is named across models: "<model>.<offer>".
func OfferURL(model, offer string) string { return model + "." + offer }

// ParseOfferURL reads "<model>.<offer>[:<endpoint>]".
func ParseOfferURL(s string) (model, offer, endpoint string, err error) {
	rest, endpoint, _ := strings.Cut(s, ":")
	model, offer, ok := strings.Cut(rest, ".")
	if !ok || model == "" || offer == "" || strings.Contains(offer, ".") {
		return "", "", "", fmt.Errorf("invalid offer %q: want <model>.<offer>[:<endpoint>]", s)
	}
	return model, offer, endpoint, nil
}

// Offer makes an offer of endpoints of an application of the model, and waits until the operator accepts it.
func (c *Client) Offer(ctx context.Context, o OfferOptions) (*v1alpha1.Offer, error) {
	if err := c.requireModel(ctx); err != nil {
		return nil, err
	}
	if o.Application == "" || len(o.Endpoints) == 0 {
		return nil, fmt.Errorf("an offer needs an application and at least one endpoint: <application>:<endpoint>[,<endpoint>]")
	}
	app, err := c.getApplication(ctx, o.Application)
	if err != nil {
		return nil, err
	}
	if ch, err := CharmOf(app); err != nil {
		return nil, err
	} else if ch != nil {
		for _, ep := range o.Endpoints {
			_, role, ok := ch.endpoint(ep)
			switch {
			case !ok:
				return nil, fmt.Errorf("application %q has no endpoint %q", o.Application, ep)
			case role == "peers":
				return nil, fmt.Errorf("endpoint %q of %q is a peer endpoint, which cannot be offered", ep, o.Application)
			}
		}
	}
	name := o.Name
	if name == "" {
		name = o.Application
	}
	if err := ValidateApplicationName(name); err != nil {
		return nil, fmt.Errorf("invalid offer name %q: %w", name, err)
	}
	allowed := o.AllowedModels
	if allowed == nil {
		cfg, err := c.ModelConfig(ctx)
		if err != nil {
			return nil, err
		}
		for _, m := range strings.Split(cfg["offer-allowed-models"], ",") {
			if m = strings.TrimSpace(m); m != "" {
				allowed = append(allowed, m)
			}
		}
	}
	offer := &v1alpha1.Offer{
		ObjectMeta: metav1.ObjectMeta{Namespace: c.Namespace, Name: name},
		Spec:       v1alpha1.OfferSpec{Application: o.Application, Endpoints: o.Endpoints, AllowedModels: allowed},
	}
	if err := c.Kube.Create(ctx, offer); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("offer %q already exists in model %q", name, c.Namespace)
		}
		return nil, err
	}
	err = c.wait(ctx, 30*time.Second, fmt.Sprintf("offer %q to be ready", name), func(ctx context.Context) (bool, string, error) {
		if err := c.Kube.Get(ctx, client.ObjectKeyFromObject(offer), offer); err != nil {
			return false, "", err
		}
		cond := meta.FindStatusCondition(offer.Status.Conditions, v1alpha1.OfferReady)
		switch {
		case cond == nil:
			return false, "", nil
		case cond.Status == metav1.ConditionTrue:
			return true, "", nil
		case cond.Reason == "CharmNotResolved":
			return false, cond.Message, nil
		}
		return false, "", fmt.Errorf("%s", cond.Message)
	})
	if err != nil {
		_ = c.Kube.Delete(ctx, offer)
		return nil, err
	}
	return offer, nil
}

// Offers lists the model's offers with their connections.
func (c *Client) Offers(ctx context.Context) ([]v1alpha1.Offer, error) {
	if err := c.requireModel(ctx); err != nil {
		return nil, err
	}
	var list v1alpha1.OfferList
	if err := c.Kube.List(ctx, &list, client.InNamespace(c.Namespace)); err != nil {
		return nil, err
	}
	sort.Slice(list.Items, func(i, j int) bool { return list.Items[i].Name < list.Items[j].Name })
	return list.Items, nil
}

// ShowOffer reads an offer of any model: "<model>.<offer>" (or just the name, for this model).
func (c *Client) ShowOffer(ctx context.Context, url string) (*v1alpha1.Offer, error) {
	ns, name := c.Namespace, url
	if strings.Contains(url, ".") {
		var err error
		if ns, name, _, err = ParseOfferURL(url); err != nil {
			return nil, err
		}
	}
	var offer v1alpha1.Offer
	if err := c.Kube.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, &offer); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("offer %q not found in model %q", name, ns)
		}
		return nil, err
	}
	return &offer, nil
}

// RemoveOffer deletes an offer of the model. With connections it refuses, unless force: the relations that consume it
// are then removed in order first.
func (c *Client) RemoveOffer(ctx context.Context, name string, force bool, timeout time.Duration) error {
	offer, err := c.ShowOffer(ctx, name)
	if err != nil {
		return err
	}
	if n := len(offer.Status.Connections); n > 0 && !force {
		var who []string
		for _, cn := range offer.Status.Connections {
			who = append(who, fmt.Sprintf("%s:%s of model %s", cn.Application, cn.Endpoint, cn.Namespace))
		}
		return fmt.Errorf("offer %q has %d connection%s (%s): use --force to remove it with them", offer.Name, n, plural(n), strings.Join(who, ", "))
	}
	if err := c.Kube.Delete(ctx, offer); err != nil {
		return client.IgnoreNotFound(err)
	}
	if timeout == 0 {
		timeout = 5 * time.Minute
	}
	return c.wait(ctx, timeout, fmt.Sprintf("offer %q to be removed", offer.Name), func(ctx context.Context) (bool, string, error) {
		err := c.Kube.Get(ctx, client.ObjectKeyFromObject(offer), &v1alpha1.Offer{})
		if apierrors.IsNotFound(err) {
			return true, "", nil
		}
		return false, "removing its connections", err
	})
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}
