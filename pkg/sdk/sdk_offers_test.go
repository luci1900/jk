package sdk

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/luci1900/jk/api/v1alpha1"
)

// readyOffers plays the operator for Offers: those named in bad get a reason, the others are ready with the endpoints.
func readyOffers(bad map[string]string, endpoints []v1alpha1.OfferEndpoint) func(ctx context.Context, c *Client) {
	return func(ctx context.Context, c *Client) {
		var offers v1alpha1.OfferList
		if err := c.Kube.List(ctx, &offers, client.InNamespace(c.Namespace)); err != nil {
			return
		}
		for i := range offers.Items {
			o := &offers.Items[i]
			if len(o.Status.Conditions) > 0 {
				continue
			}
			cond := metav1.Condition{Type: v1alpha1.OfferReady, Status: metav1.ConditionTrue, Reason: "Ready"}
			if msg, ok := bad[o.Name]; ok {
				cond.Status, cond.Reason, cond.Message = metav1.ConditionFalse, "EndpointNotFound", msg
			} else {
				o.Status.Endpoints = endpoints
			}
			meta.SetStatusCondition(&o.Status.Conditions, cond)
			_ = c.Kube.Status().Update(ctx, o)
		}
	}
}

func TestOffer(t *testing.T) {
	ctx := context.Background()
	eps := []v1alpha1.OfferEndpoint{{Name: "database", Interface: "pgsql", Role: "provider"}}
	t.Run("an offer is made and waits for the operator", func(t *testing.T) {
		c, _ := newClient(t, resolved("pg", "db", dbMetadata, dbConfig, "", 1))
		withOperator(t, c, readyOffers(nil, eps))
		o, err := c.Offer(ctx, OfferOptions{Application: "pg", Endpoints: []string{"database"}, AllowedModels: []string{"web"}})
		noErr(t, err)
		if o.Name != "pg" || o.Spec.Application != "pg" || o.Spec.AllowedModels[0] != "web" || len(o.Status.Endpoints) != 1 {
			t.Fatalf("%+v", o)
		}
		_, err = c.Offer(ctx, OfferOptions{Application: "pg", Endpoints: []string{"database"}})
		wantErr(t, err, `offer "pg" already exists`)
		list, err := c.Offers(ctx)
		noErr(t, err)
		if len(list) != 1 {
			t.Fatalf("%v", list)
		}
	})
	t.Run("the default allowed models are the model config's", func(t *testing.T) {
		c, _ := newClient(t, resolved("pg", "db", dbMetadata, dbConfig, "", 1))
		withOperator(t, c, readyOffers(nil, eps))
		noErr(t, c.SetModelConfig(ctx, map[string]string{"offer-allowed-models": "web, cos"}))
		o, err := c.Offer(ctx, OfferOptions{Application: "pg", Endpoints: []string{"database"}, Name: "shared"})
		noErr(t, err)
		if o.Name != "shared" || len(o.Spec.AllowedModels) != 2 || o.Spec.AllowedModels[1] != "cos" {
			t.Fatalf("%+v", o.Spec)
		}
		// Explicitly none overrides the default.
		o, err = c.Offer(ctx, OfferOptions{Application: "pg", Endpoints: []string{"database"}, Name: "private", AllowedModels: []string{}})
		noErr(t, err)
		if len(o.Spec.AllowedModels) != 0 {
			t.Fatalf("%+v", o.Spec)
		}
	})
	t.Run("checked before creating", func(t *testing.T) {
		c, _ := newClient(t, resolved("pg", "db", dbMetadata, dbConfig, "", 1))
		_, err := c.Offer(ctx, OfferOptions{Application: "pg", Endpoints: []string{"nope"}})
		wantErr(t, err, `no endpoint "nope"`)
		_, err = c.Offer(ctx, OfferOptions{Application: "pg", Endpoints: []string{"cluster"}})
		wantErr(t, err, "peer endpoint")
		_, err = c.Offer(ctx, OfferOptions{Application: "nothing", Endpoints: []string{"database"}})
		wantErr(t, err, `application "nothing" not found`)
		_, err = c.Offer(ctx, OfferOptions{Application: "pg"})
		wantErr(t, err, "at least one endpoint")
		_, err = c.Offer(ctx, OfferOptions{Application: "pg", Endpoints: []string{"database"}, Name: "Bad_Name"})
		wantErr(t, err, "invalid offer name")
		list, _ := c.Offers(ctx)
		if len(list) != 0 {
			t.Fatalf("an offer was created: %v", list)
		}
	})
	t.Run("an offer the operator rejects is removed again", func(t *testing.T) {
		c, _ := newClient(t, resolved("pg", "db", dbMetadata, dbConfig, "", 1))
		withOperator(t, c, readyOffers(map[string]string{"pg": "the operator disagrees"}, nil))
		_, err := c.Offer(ctx, OfferOptions{Application: "pg", Endpoints: []string{"database"}})
		wantErr(t, err, "the operator disagrees")
		list, _ := c.Offers(ctx)
		if len(list) != 0 {
			t.Fatalf("%v", list)
		}
	})
}

func TestOfferURLs(t *testing.T) {
	m, o, e, err := ParseOfferURL("db.pg:database")
	if err != nil || m != "db" || o != "pg" || e != "database" {
		t.Fatalf("%s %s %s %v", m, o, e, err)
	}
	if m, o, e, err = ParseOfferURL("db.pg"); err != nil || e != "" || m != "db" || o != "pg" {
		t.Fatalf("%s %s %s %v", m, o, e, err)
	}
	for _, bad := range []string{"pg", ".pg", "db.", "a.b.c", ""} {
		if _, _, _, err := ParseOfferURL(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	if OfferURL("db", "pg") != "db.pg" {
		t.Fatal("OfferURL")
	}
	for in, want := range map[string]bool{"db.pg": true, "db.pg:database": true, "pg": false, "pg:database": false, "pg.x:y": true} {
		if isOfferURL(in) != want {
			t.Errorf("isOfferURL(%q)", in)
		}
	}
}

func TestShowAndRemoveOffer(t *testing.T) {
	ctx := context.Background()
	offer := &v1alpha1.Offer{ObjectMeta: metav1.ObjectMeta{Namespace: "m", Name: "pg"}, Spec: v1alpha1.OfferSpec{Application: "pg", Endpoints: []string{"database"}},
		Status: v1alpha1.OfferStatus{Connections: []v1alpha1.OfferConnection{{Namespace: "web", Relation: "r", Application: "client", Endpoint: "db", Status: "joined"}}}}
	idle := &v1alpha1.Offer{ObjectMeta: metav1.ObjectMeta{Namespace: "m", Name: "idle"}, Spec: v1alpha1.OfferSpec{Application: "pg", Endpoints: []string{"database"}}}
	c, _ := newClient(t, offer, idle)
	got, err := c.ShowOffer(ctx, "m.pg")
	noErr(t, err)
	if got.Name != "pg" {
		t.Fatal(got)
	}
	if got, err = c.ShowOffer(ctx, "pg"); err != nil || got.Name != "pg" {
		t.Fatalf("%v %v", got, err)
	}
	_, err = c.ShowOffer(ctx, "m.nope")
	wantErr(t, err, `offer "nope" not found in model "m"`)
	_, err = c.ShowOffer(ctx, "a.b.c")
	wantErr(t, err, "invalid offer")

	err = c.RemoveOffer(ctx, "pg", false, time.Second)
	wantErr(t, err, "has 1 connection (client:db of model web)")
	wantErr(t, err, "--force")
	noErr(t, c.RemoveOffer(ctx, "idle", false, time.Second))
	noErr(t, c.RemoveOffer(ctx, "pg", true, time.Second))
	wantErr(t, c.RemoveOffer(ctx, "pg", true, time.Second), "not found")
}

// offered is an Offer of model "far" as the operator publishes it.
func offered(app string, eps ...v1alpha1.OfferEndpoint) *v1alpha1.Offer {
	var names []string
	for _, e := range eps {
		names = append(names, e.Name)
	}
	return &v1alpha1.Offer{
		ObjectMeta: metav1.ObjectMeta{Namespace: "far", Name: "pg"},
		Spec:       v1alpha1.OfferSpec{Application: app, Endpoints: names},
		Status:     v1alpha1.OfferStatus{Endpoints: eps, Conditions: []metav1.Condition{{Type: v1alpha1.OfferReady, Status: metav1.ConditionTrue, Reason: "Ready"}}},
	}
}

func TestIntegrateWithAnOffer(t *testing.T) {
	ctx := context.Background()
	provider := func(name, iface string) v1alpha1.OfferEndpoint {
		return v1alpha1.OfferEndpoint{Name: name, Interface: iface, Role: "provider"}
	}
	web := resolved("web", "web", webMetadata, "", "", 1) // requires db (pgsql, limit 1) and scrape (prometheus_scrape)
	t.Run("the compatible endpoint is found and the relation is made", func(t *testing.T) {
		c, _ := newClient(t, web, offered("db", provider("database", "pgsql"), provider("metrics", "prometheus_scrape"), provider("other", "x")))
		withOperator(t, c, readyRelations(nil))
		// web:db and web:scrape each fit one offered endpoint: ambiguous without naming one.
		_, err := c.Integrate(ctx, "web", "far.pg")
		wantErr(t, err, "ambiguous relation")
		wantErr(t, err, "web:db far.pg:database")
		wantErr(t, err, "web:scrape far.pg:metrics")
		rel, err := c.Integrate(ctx, "web:db", "far.pg", IntegrateOptions{})
		noErr(t, err)
		if rel.Name != "web.db-pg.database" || rel.Spec.Offer != "pg" || rel.Spec.Alias != "" || rel.Namespace != "m" ||
			rel.Spec.Endpoints[0] != (v1alpha1.EndpointRef{Namespace: "m", Application: "web", Endpoint: "db"}) ||
			rel.Spec.Endpoints[1] != (v1alpha1.EndpointRef{Namespace: "far", Application: "db", Endpoint: "database"}) {
			t.Fatalf("%+v", rel)
		}
		_, err = c.Integrate(ctx, "web:db", "far.pg")
		wantErr(t, err, "already exists")
		// The offer may come first on the command line, as for any relation, and an endpoint of the offer narrows it.
		rel, err = c.Integrate(ctx, "far.pg:metrics", "web:scrape", IntegrateOptions{Alias: "prom"})
		noErr(t, err)
		if rel.Spec.Alias != "prom" || rel.Name != "web.scrape-prom.metrics" {
			t.Fatalf("%+v", rel.Spec)
		}
	})
	t.Run("problems are said before anything is created", func(t *testing.T) {
		web2 := resolved("pg", "web", webMetadata, "", "", 1) // an application with the offer's name
		c, _ := newClient(t, web, web2, offered("db", provider("database", "pgsql")))
		_, err := c.Integrate(ctx, "web:scrape", "far.pg")
		wantErr(t, err, "no compatible endpoints")
		_, err = c.Integrate(ctx, "web:db", "far.pg:nope")
		wantErr(t, err, "no compatible endpoints")
		_, err = c.Integrate(ctx, "web:db", "far.pg")
		wantErr(t, err, `choose another with --alias`) // the offer's name is an application here
		_, err = c.Integrate(ctx, "web", "m.pg")
		wantErr(t, err, "is in this model")
		_, err = c.Integrate(ctx, "far.pg", "far.pg")
		wantErr(t, err, "cannot relate two offers")
		_, err = c.Integrate(ctx, "web", "far.ghost")
		wantErr(t, err, `offer "ghost" not found in model "far"`)
		_, err = c.Integrate(ctx, "nothing", "far.pg")
		wantErr(t, err, `application "nothing" not found`)
		_, err = c.Integrate(ctx, "web", "web2", IntegrateOptions{Alias: "x"})
		wantErr(t, err, "--alias applies to offers")
		var rels v1alpha1.RelationList
		noErr(t, c.Kube.List(ctx, &rels, client.InNamespace("m")))
		if len(rels.Items) != 0 {
			t.Fatalf("created %d relations", len(rels.Items))
		}
	})
	t.Run("an offer that is not ready", func(t *testing.T) {
		o := offered("db")
		o.Status.Conditions = []metav1.Condition{{Type: v1alpha1.OfferReady, Status: metav1.ConditionFalse, Reason: "CharmNotResolved", Message: "the charm is not resolved yet"}}
		c, _ := newClient(t, web, o)
		_, err := c.Integrate(ctx, "web", "far.pg")
		wantErr(t, err, "is not ready: the charm is not resolved yet")
	})
	t.Run("the alias must be free", func(t *testing.T) {
		other := &v1alpha1.Relation{ObjectMeta: metav1.ObjectMeta{Namespace: "m", Name: "x"}, Spec: v1alpha1.RelationSpec{Offer: "else", Alias: "pg",
			Endpoints: []v1alpha1.EndpointRef{{Namespace: "m", Application: "web", Endpoint: "scrape"}, {Namespace: "far2", Application: "p", Endpoint: "m"}}}}
		c, _ := newClient(t, web, other, offered("db", provider("database", "pgsql")))
		_, err := c.Integrate(ctx, "web:db", "far.pg")
		wantErr(t, err, `"pg" is used by another relation`)
	})
	t.Run("the operator's rejection comes back and the relation is removed", func(t *testing.T) {
		c, _ := newClient(t, web, offered("db", provider("database", "pgsql")))
		withOperator(t, c, readyRelations(map[string]string{"web.db-pg.database": "offer not allowed"}))
		_, err := c.Integrate(ctx, "web:db", "far.pg")
		wantErr(t, err, "offer not allowed")
		var rels v1alpha1.RelationList
		noErr(t, c.Kube.List(ctx, &rels, client.InNamespace("m")))
		if len(rels.Items) != 0 {
			t.Fatalf("the rejected relation is still there")
		}
	})
}

func TestRemoveRelationByAlias(t *testing.T) {
	ctx := context.Background()
	rel := &v1alpha1.Relation{ObjectMeta: metav1.ObjectMeta{Namespace: "m", Name: "r"}, Spec: v1alpha1.RelationSpec{Offer: "pg", Alias: "remote-pg",
		Endpoints: []v1alpha1.EndpointRef{{Namespace: "m", Application: "web", Endpoint: "db"}, {Namespace: "far", Application: "db", Endpoint: "database"}}}}
	mirror := &v1alpha1.Relation{ObjectMeta: metav1.ObjectMeta{Namespace: "m", Name: "remote.far.x", Labels: map[string]string{v1alpha1.RemoteLabel: "true"}},
		Spec: v1alpha1.RelationSpec{Alias: "remote-abc", Endpoints: []v1alpha1.EndpointRef{{Namespace: "far", Application: "c", Endpoint: "x"}, {Namespace: "m", Application: "web", Endpoint: "db"}}}}
	c, _ := newClient(t, rel, mirror)
	wantErr(t, c.RemoveRelation(ctx, "web", "db"), "no relation found")         // the real application's name is not the alias
	wantErr(t, c.RemoveRelation(ctx, "web", "remote-abc"), "no relation found") // mirrors belong to the operator
	noErr(t, c.RemoveRelation(ctx, "web", "remote-pg"))
	var left v1alpha1.RelationList
	noErr(t, c.Kube.List(ctx, &left, client.InNamespace("m")))
	if len(left.Items) != 1 || left.Items[0].Name != "remote.far.x" {
		t.Fatalf("%v", left.Items)
	}
}

func TestStatusShowsOffersAndRemoteApplications(t *testing.T) {
	ctx := context.Background()
	pg := resolved("pg", "db", dbMetadata, "", "", 1)
	offer := &v1alpha1.Offer{ObjectMeta: metav1.ObjectMeta{Namespace: "m", Name: "pg-offer"}, Spec: v1alpha1.OfferSpec{Application: "pg", Endpoints: []string{"database"}, AllowedModels: []string{"web"}},
		Status: v1alpha1.OfferStatus{Endpoints: []v1alpha1.OfferEndpoint{{Name: "database", Interface: "pgsql", Role: "provider"}},
			Connections: []v1alpha1.OfferConnection{{Namespace: "web", Relation: "r", Application: "client", Endpoint: "db", Status: "joined"}},
			Conditions:  []metav1.Condition{{Type: v1alpha1.OfferReady, Status: metav1.ConditionTrue, Reason: "Ready"}}}}
	web := resolved("web", "web", webMetadata, "", "", 1)
	consumed := &v1alpha1.Relation{ObjectMeta: metav1.ObjectMeta{Namespace: "m", Name: "r"}, Spec: v1alpha1.RelationSpec{Offer: "cos-prom",
		Endpoints: []v1alpha1.EndpointRef{{Namespace: "m", Application: "web", Endpoint: "scrape"}, {Namespace: "cos", Application: "prometheus", Endpoint: "metrics"}}},
		Status: v1alpha1.RelationStatus{ID: 2, Conditions: []metav1.Condition{{Type: v1alpha1.RelationReady, Status: metav1.ConditionTrue, Reason: "Ready"}, {Type: v1alpha1.RelationValid, Status: metav1.ConditionTrue, Reason: "Valid"}}}}
	mirror := &v1alpha1.Relation{ObjectMeta: metav1.ObjectMeta{Namespace: "m", Name: "remote.web.r", Labels: map[string]string{v1alpha1.RemoteLabel: "true"}},
		Spec:   v1alpha1.RelationSpec{Alias: "remote-1a2b", Endpoints: []v1alpha1.EndpointRef{{Namespace: "web", Application: "client", Endpoint: "db"}, {Namespace: "m", Application: "pg", Endpoint: "database"}}},
		Status: v1alpha1.RelationStatus{ID: 9, Conditions: []metav1.Condition{{Type: v1alpha1.RelationReady, Status: metav1.ConditionTrue, Reason: "Ready"}, {Type: v1alpha1.RelationValid, Status: metav1.ConditionTrue, Reason: "Valid"}}}}
	c, _ := newClient(t, pg, web, offer, consumed, mirror, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "x", Namespace: "m"}})
	st, err := c.Status(ctx)
	noErr(t, err)
	o := st.Offers["pg-offer"]
	if o.Application != "pg" || !o.Ready || len(o.Connections) != 1 || len(o.Endpoints) != 1 || o.Allowed[0] != "web" {
		t.Fatalf("%+v", o)
	}
	ra, ok := st.RemoteApplications["cos-prom"]
	if !ok || ra.Offer != "cos.cos-prom" || ra.Status != "joined" || len(ra.Endpoints) != 1 || ra.Endpoints[0] != "web:scrape" {
		t.Fatalf("%+v", st.RemoteApplications)
	}
	if _, mirrored := st.RemoteApplications["remote-1a2b"]; mirrored {
		t.Fatal("the mirror is shown as consumed application")
	}
	var consumedRel, mirrorRel RelationStatus
	for _, r := range st.Relations {
		if r.Provider == "cos-prom:metrics" || r.Requirer == "cos-prom:metrics" {
			consumedRel = r
		}
		if r.Provider == "pg:database" {
			mirrorRel = r
		}
	}
	// A relation to another model shows the remote application by its alias.
	if consumedRel.Requirer != "web:scrape" || consumedRel.Provider != "cos-prom:metrics" || consumedRel.Interface != "prometheus_scrape" || consumedRel.Status != "joined" {
		t.Fatalf("%+v", st.Relations)
	}
	if mirrorRel.Requirer != "remote-1a2b:db" || mirrorRel.Interface != "pgsql" {
		t.Fatalf("%+v", st.Relations)
	}
}
