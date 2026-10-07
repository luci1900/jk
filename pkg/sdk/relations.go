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
	"github.com/luci1900/jk/internal/operator"
)

// EndpointSpec is an endpoint as the user types it: "app" or "app:endpoint".
type EndpointSpec struct{ App, Endpoint string }

// ParseEndpoint reads "app" or "app:endpoint".
func ParseEndpoint(s string) (EndpointSpec, error) {
	app, ep, _ := strings.Cut(s, ":")
	if app == "" || strings.Contains(ep, ":") {
		return EndpointSpec{}, fmt.Errorf("invalid endpoint %q: want <application> or <application>:<endpoint>", s)
	}
	return EndpointSpec{App: app, Endpoint: ep}, nil
}

func (e EndpointSpec) String() string {
	if e.Endpoint == "" {
		return e.App
	}
	return e.App + ":" + e.Endpoint
}

// candidatePairs lists the endpoint pairs of two applications that can be related: one side provides, the other
// requires, with the same interface.
func candidatePairs(a, b EndpointSpec, ca, cb *Charm) [][2]EndpointSpec {
	var out [][2]EndpointSpec
	// fits says whether an endpoint name is the one the user asked for (any, when they gave none).
	fits := func(want EndpointSpec, name string) bool { return want.Endpoint == "" || want.Endpoint == name }
	for pn, p := range ca.Provides {
		for rn, r := range cb.Requires {
			if fits(a, pn) && fits(b, rn) && p.Interface == r.Interface {
				out = append(out, [2]EndpointSpec{{a.App, pn}, {b.App, rn}})
			}
		}
	}
	for pn, p := range cb.Provides {
		for rn, r := range ca.Requires {
			if fits(b, pn) && fits(a, rn) && p.Interface == r.Interface {
				out = append(out, [2]EndpointSpec{{a.App, rn}, {b.App, pn}})
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i][0].String()+out[i][1].String() < out[j][0].String()+out[j][1].String()
	})
	return out
}

// relationName names the Relation object after its endpoints, provider first.
func relationName(eps [2]EndpointSpec) string {
	return strings.ReplaceAll(eps[0].App+"."+eps[0].Endpoint+"-"+eps[1].App+"."+eps[1].Endpoint, "_", "-")
}

// IntegrateOptions are the optional parts of Integrate.
type IntegrateOptions struct {
	// Alias is the name the offered application has in this model (default: the offer's name); for offers only.
	Alias string
}

// isOfferURL says whether an integrate argument names an offer ("<model>.<offer>[:<endpoint>]") rather than an
// application: application names have no dots.
func isOfferURL(s string) bool {
	app, _, _ := strings.Cut(s, ":")
	return strings.Contains(app, ".")
}

// Integrate relates two applications and waits until the operator accepts the relation. Without endpoints it picks
// the one compatible pair, as juju does. One of the arguments may be an offer of another model, "<model>.<offer>".
func (c *Client) Integrate(ctx context.Context, first, second string, opts ...IntegrateOptions) (*v1alpha1.Relation, error) {
	var o IntegrateOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	switch {
	case isOfferURL(first) && isOfferURL(second):
		return nil, fmt.Errorf("cannot relate two offers: one side must be an application of this model")
	case isOfferURL(second):
		return c.integrateOffer(ctx, first, second, o)
	case isOfferURL(first):
		return c.integrateOffer(ctx, second, first, o)
	case o.Alias != "":
		return nil, fmt.Errorf("--alias applies to offers of other models")
	}
	a, err := ParseEndpoint(first)
	if err != nil {
		return nil, err
	}
	b, err := ParseEndpoint(second)
	if err != nil {
		return nil, err
	}
	if a.App == b.App {
		return nil, fmt.Errorf("cannot relate application %q to itself", a.App)
	}
	apps := map[string]*v1alpha1.Application{}
	charms := map[string]*Charm{}
	for _, name := range []string{a.App, b.App} {
		app, err := c.getApplication(ctx, name)
		if err != nil {
			return nil, err
		}
		// The charm is known as soon as the operator has resolved it.
		err = c.wait(ctx, 30*time.Second, fmt.Sprintf("the charm of %q to be resolved", name), func(ctx context.Context) (bool, string, error) {
			if app, err = c.getApplication(ctx, name); err != nil {
				return false, "", err
			}
			return app.Status.Charm != nil && app.Status.Charm.Metadata != nil, conditionMessage(app), nil
		})
		if err != nil {
			return nil, err
		}
		apps[operator.AppKey(c.Namespace, name)] = app
		if charms[name], err = CharmOf(app); err != nil {
			return nil, err
		}
	}
	pairs := candidatePairs(a, b, charms[a.App], charms[b.App])
	switch len(pairs) {
	case 0:
		return nil, fmt.Errorf("no compatible endpoints between %s and %s", a, b)
	case 1:
	default:
		var opts []string
		for _, p := range pairs {
			opts = append(opts, p[0].String()+" "+p[1].String())
		}
		return nil, fmt.Errorf("ambiguous relation: %s %s could refer to:\n  %s", a, b, strings.Join(opts, "\n  "))
	}
	eps := pairs[0]
	refs := []v1alpha1.EndpointRef{
		{Namespace: c.Namespace, Application: eps[0].App, Endpoint: eps[0].Endpoint},
		{Namespace: c.Namespace, Application: eps[1].App, Endpoint: eps[1].Endpoint},
	}
	var existing v1alpha1.RelationList
	if err := c.Kube.List(ctx, &existing, client.InNamespace(c.Namespace)); err != nil {
		return nil, err
	}
	if p := operator.CheckRelation(c.Namespace, refs, apps, existing.Items); p != nil {
		if p.Reason == operator.ReasonDuplicate {
			return nil, fmt.Errorf("relation %s %s already exists", eps[0], eps[1])
		}
		return nil, fmt.Errorf("%s", p.Message)
	}
	rel := &v1alpha1.Relation{
		ObjectMeta: metav1.ObjectMeta{Name: relationName(eps), Namespace: c.Namespace},
		Spec:       v1alpha1.RelationSpec{Endpoints: refs},
	}
	if err := c.Kube.Create(ctx, rel); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("relation %s %s already exists", eps[0], eps[1])
		}
		return nil, err
	}
	return rel, c.waitRelation(ctx, rel)
}

// waitRelation waits until the operator has established the relation; one it rejects is deleted again, with its reason.
func (c *Client) waitRelation(ctx context.Context, rel *v1alpha1.Relation) error {
	err := c.wait(ctx, time.Minute, "the relation to be established", func(ctx context.Context) (bool, string, error) {
		if err := c.Kube.Get(ctx, client.ObjectKeyFromObject(rel), rel); err != nil {
			return false, "", err
		}
		cond := meta.FindStatusCondition(rel.Status.Conditions, v1alpha1.RelationReady)
		switch {
		case cond == nil:
			return false, "", nil
		case cond.Status == metav1.ConditionTrue:
			return true, "", nil
		case cond.Reason == "Pending":
			return false, cond.Message, nil
		}
		return false, "", fmt.Errorf("%s", cond.Message)
	})
	if err != nil {
		_ = c.Kube.Delete(ctx, rel)
	}
	return err
}

// integrateOffer relates an application of this model to an offer of another one.
func (c *Client) integrateOffer(ctx context.Context, local, url string, o IntegrateOptions) (*v1alpha1.Relation, error) {
	a, err := ParseEndpoint(local)
	if err != nil {
		return nil, err
	}
	model, offerName, remoteEP, err := ParseOfferURL(url)
	if err != nil {
		return nil, err
	}
	if model == c.Namespace {
		return nil, fmt.Errorf("offer %q is in this model: relate the application %q directly", url, offerName)
	}
	app, err := c.getApplication(ctx, a.App)
	if err != nil {
		return nil, err
	}
	err = c.wait(ctx, 30*time.Second, fmt.Sprintf("the charm of %q to be resolved", a.App), func(ctx context.Context) (bool, string, error) {
		if app, err = c.getApplication(ctx, a.App); err != nil {
			return false, "", err
		}
		return app.Status.Charm != nil && app.Status.Charm.Metadata != nil, conditionMessage(app), nil
	})
	if err != nil {
		return nil, err
	}
	ch, err := CharmOf(app)
	if err != nil {
		return nil, err
	}
	offer, err := c.ShowOffer(ctx, model+"."+offerName)
	if err != nil {
		return nil, err
	}
	if len(offer.Status.Endpoints) == 0 {
		return nil, fmt.Errorf("offer %q is not ready: %s", url, offerNotReady(offer))
	}
	remote := &Charm{Provides: map[string]Endpoint{}, Requires: map[string]Endpoint{}}
	for _, e := range offer.Status.Endpoints {
		if e.Role == "provider" {
			remote.Provides[e.Name] = Endpoint{Interface: e.Interface}
		} else {
			remote.Requires[e.Name] = Endpoint{Interface: e.Interface}
		}
	}
	pairs := candidatePairs(a, EndpointSpec{App: offer.Spec.Application, Endpoint: remoteEP}, ch, remote)
	switch len(pairs) {
	case 0:
		return nil, fmt.Errorf("no compatible endpoints between %s and offer %s", a, url)
	case 1:
	default:
		var opts []string
		for _, p := range pairs {
			opts = append(opts, p[0].String()+" "+model+"."+offerName+":"+p[1].Endpoint)
		}
		return nil, fmt.Errorf("ambiguous relation: %s %s could refer to:\n  %s", a, url, strings.Join(opts, "\n  "))
	}
	alias := o.Alias
	if alias == "" {
		alias = offerName
	}
	eps := pairs[0]
	refs := []v1alpha1.EndpointRef{
		{Namespace: c.Namespace, Application: eps[0].App, Endpoint: eps[0].Endpoint},
		{Namespace: model, Application: eps[1].App, Endpoint: eps[1].Endpoint},
	}
	var existing v1alpha1.RelationList
	if err := c.Kube.List(ctx, &existing, client.InNamespace(c.Namespace)); err != nil {
		return nil, err
	}
	var apps v1alpha1.ApplicationList
	if err := c.Kube.List(ctx, &apps, client.InNamespace(c.Namespace)); err != nil {
		return nil, err
	}
	for _, ap := range apps.Items {
		if ap.Name == alias {
			return nil, fmt.Errorf("the application %q of this model has the name %q: choose another with --alias", ap.Name, alias)
		}
	}
	for _, r := range existing.Items {
		if r.Spec.Offer == offerName && r.Spec.Endpoints[1].Namespace == model && r.Spec.Endpoints[0] == refs[0] {
			return nil, fmt.Errorf("relation %s %s already exists", eps[0], url)
		}
		if r.DeletionTimestamp == nil && aliasOfRelation(&r) == alias && !(r.Spec.Offer == offerName && r.Spec.Endpoints[1].Namespace == model) {
			return nil, fmt.Errorf("the name %q is used by another relation: choose another with --alias", alias)
		}
	}
	rel := &v1alpha1.Relation{
		ObjectMeta: metav1.ObjectMeta{Name: strings.ReplaceAll(eps[0].App+"."+eps[0].Endpoint+"-"+alias+"."+eps[1].Endpoint, "_", "-"), Namespace: c.Namespace},
		Spec:       v1alpha1.RelationSpec{Endpoints: refs, Offer: offerName, Alias: o.Alias},
	}
	if err := c.Kube.Create(ctx, rel); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("relation %s %s already exists", eps[0], url)
		}
		return nil, err
	}
	return rel, c.waitRelation(ctx, rel)
}

func aliasOfRelation(r *v1alpha1.Relation) string {
	if r.Spec.Alias != "" {
		return r.Spec.Alias
	}
	return r.Spec.Offer
}

func offerNotReady(o *v1alpha1.Offer) string {
	if cond := meta.FindStatusCondition(o.Status.Conditions, v1alpha1.OfferReady); cond != nil && cond.Status != metav1.ConditionTrue {
		return cond.Message
	}
	return "its application has no resolved charm yet"
}

// RemoveRelation deletes the relation between two endpoints; the units run their departed and broken hooks first.
func (c *Client) RemoveRelation(ctx context.Context, first, second string) error {
	a, err := ParseEndpoint(first)
	if err != nil {
		return err
	}
	b, err := ParseEndpoint(second)
	if err != nil {
		return err
	}
	var rels v1alpha1.RelationList
	if err := c.Kube.List(ctx, &rels, client.InNamespace(c.Namespace)); err != nil {
		return err
	}
	var found []*v1alpha1.Relation
	for i := range rels.Items {
		r := &rels.Items[i]
		if len(r.Spec.Endpoints) != 2 || r.DeletionTimestamp != nil || r.Labels[v1alpha1.RemoteLabel] == "true" {
			continue
		}
		// An application of another model is known here by its alias.
		match := func(ref v1alpha1.EndpointRef, e EndpointSpec) bool {
			name := ref.Application
			if ref.Namespace != r.Namespace {
				name = aliasOfRelation(r)
			}
			return name == e.App && (e.Endpoint == "" || e.Endpoint == ref.Endpoint)
		}
		e0, e1 := r.Spec.Endpoints[0], r.Spec.Endpoints[1]
		if (match(e0, a) && match(e1, b)) || (match(e0, b) && match(e1, a)) {
			found = append(found, r)
		}
	}
	switch len(found) {
	case 0:
		return fmt.Errorf("no relation found between %s and %s", a, b)
	case 1:
		return c.Kube.Delete(ctx, found[0])
	}
	var names []string
	for _, r := range found {
		names = append(names, fmt.Sprintf("%s:%s %s:%s", r.Spec.Endpoints[0].Application, r.Spec.Endpoints[0].Endpoint, r.Spec.Endpoints[1].Application, r.Spec.Endpoints[1].Endpoint))
	}
	return fmt.Errorf("ambiguous relation: %s %s could refer to:\n  %s", a, b, strings.Join(names, "\n  "))
}
