package envtest

import (
	"context"
	"strconv"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/internal/operator"
)

func newOffer(t *testing.T, c client.Client, ns, name, app string, endpoints []string, allowed ...string) *v1alpha1.Offer {
	t.Helper()
	o := &v1alpha1.Offer{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}, Spec: v1alpha1.OfferSpec{Application: app, Endpoints: endpoints, AllowedModels: allowed}}
	if err := c.Create(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	return o
}

func consume(t *testing.T, c client.Client, name, consumerNS, clientApp, clientEP, offerNS, offerApp, offerEP, offer, alias string) *v1alpha1.Relation {
	t.Helper()
	rel := &v1alpha1.Relation{
		ObjectMeta: metav1.ObjectMeta{Namespace: consumerNS, Name: name},
		Spec: v1alpha1.RelationSpec{
			Endpoints: []v1alpha1.EndpointRef{{Namespace: consumerNS, Application: clientApp, Endpoint: clientEP}, {Namespace: offerNS, Application: offerApp, Endpoint: offerEP}},
			Offer:     offer, Alias: alias,
		},
	}
	if err := c.Create(context.Background(), rel); err != nil {
		t.Fatal(err)
	}
	return rel
}

func relationOf(c client.Client, ns, name string) *v1alpha1.Relation {
	var r v1alpha1.Relation
	if get(c, ns, name, &r) != nil {
		return nil
	}
	return &r
}

// unitInRelation creates (or updates) UnitData app-n with its settings in the relation of the given id.
func unitInRelation(t *testing.T, c client.Client, ns, app string, n int, id int64, inScope bool, data map[string]string) {
	t.Helper()
	name := app + "-" + strconv.Itoa(n)
	var ud v1alpha1.UnitData
	err := get(c, ns, name, &ud)
	if err != nil {
		ud = v1alpha1.UnitData{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}}
	}
	if ud.Spec.Relations == nil {
		ud.Spec.Relations = map[string]v1alpha1.UnitRelationState{}
	}
	ud.Spec.Relations[strconv.FormatInt(id, 10)] = v1alpha1.UnitRelationState{InScope: inScope, Data: data}
	if err != nil {
		err = c.Create(context.Background(), &ud)
	} else {
		err = c.Update(context.Background(), &ud)
	}
	if err != nil {
		t.Fatal(err)
	}
}

func remoteData(c client.Client, ns, name string) *v1alpha1.RemoteData {
	var rd v1alpha1.RemoteData
	if get(c, ns, name, &rd) != nil {
		return nil
	}
	return &rd
}

func TestOfferedRelation(t *testing.T) {
	t.Parallel()
	c := startOperator(t)
	addRelationCharms()
	ctx := context.Background()
	offering := newNamespace(t, c, true)
	consumer := newNamespace(t, c, true)
	newApp(t, c, offering, "db", digestProvider, 1)
	newApp(t, c, consumer, "client", digestClient, 1)
	for _, a := range []struct{ ns, name string }{{offering, "db"}, {consumer, "client"}} {
		eventually(t, a.name+" charm resolved", func() bool { r := ready(c, a.ns, a.name); return r != nil && r.Status.Charm != nil })
	}
	offer := newOffer(t, c, offering, "pg", "db", []string{"database"}, consumer)

	// Not valid until the Offer is ready... it exists already: the relation is accepted, each side with its own id.
	rel := consume(t, c, "client.db-pg", consumer, "client", "db", offering, "db", "database", "pg", "")
	var id, remoteID int64
	eventually(t, "valid with an id on each side", func() bool {
		r := relationOf(c, consumer, rel.Name)
		if r == nil || r.Status.ID == 0 || r.Status.RemoteID == 0 {
			return false
		}
		id, remoteID = r.Status.ID, r.Status.RemoteID
		return meta.IsStatusConditionTrue(r.Status.Conditions, v1alpha1.RelationValid) && meta.IsStatusConditionTrue(r.Status.Conditions, v1alpha1.RelationReady)
	})
	mirrorName := "remote." + consumer + "." + rel.Name
	var mirror *v1alpha1.Relation
	eventually(t, "the mirror in the offering namespace", func() bool {
		mirror = relationOf(c, offering, mirrorName)
		return mirror != nil && mirror.Status.ID == remoteID && mirror.Status.RemoteID == id
	})
	if mirror.Labels[v1alpha1.RemoteLabel] != "true" || mirror.Annotations[v1alpha1.RemoteOfAnnotation] != consumer+"/"+rel.Name ||
		!strings.HasPrefix(mirror.Spec.Alias, "remote-") || mirror.Spec.Offer != "pg" || len(mirror.Spec.Endpoints) != 2 {
		t.Fatalf("mirror %+v", mirror)
	}
	if !meta.IsStatusConditionTrue(mirror.Status.Conditions, v1alpha1.RelationReady) {
		t.Fatalf("mirror conditions %+v", mirror.Status.Conditions)
	}
	if consumerCM, offeringCM := counter(t, c, consumer), counter(t, c, offering); consumerCM < id || offeringCM < remoteID {
		t.Fatalf("counters %d %d for ids %d %d", consumerCM, offeringCM, id, remoteID)
	}

	// The Offer shows the connection and the access list names the consumer.
	eventually(t, "the offer's status", func() bool {
		var o v1alpha1.Offer
		if get(c, offering, "pg", &o) != nil || len(o.Status.Connections) != 1 {
			return false
		}
		cn := o.Status.Connections[0]
		return cn.Namespace == consumer && cn.Relation == rel.Name && cn.Application == "client" && cn.Endpoint == "db" && cn.Status == "joined" &&
			meta.IsStatusConditionTrue(o.Status.Conditions, v1alpha1.OfferReady) && controllerutilHasFinalizer(&o) &&
			len(o.Status.Endpoints) == 1 && o.Status.Endpoints[0] == v1alpha1.OfferEndpoint{Name: "database", Interface: "pg", Role: "provider"}
	})
	eventually(t, "the access ConfigMap", func() bool {
		var cm corev1.ConfigMap
		if get(c, v1alpha1.SystemNamespace, v1alpha1.OfferAccessConfigMap, &cm) != nil {
			return false
		}
		_, ok := cm.Data[operator.OfferAccessKey(offering, consumer)]
		return ok
	})

	// Data crosses by the operator: the offering unit's settings reach the consumer, and the consumer's reach the offering side.
	unitInRelation(t, c, offering, "db", 0, remoteID, true, map[string]string{"host": "10.0.0.1"})
	unitInRelation(t, c, offering, "db", 1, remoteID, false, map[string]string{"host": "left"})
	unitInRelation(t, c, consumer, "client", 0, id, true, map[string]string{"who": "client-0"})
	ad := &v1alpha1.AppData{ObjectMeta: metav1.ObjectMeta{Namespace: offering, Name: "db"},
		Spec: v1alpha1.AppDataSpec{Relations: map[string]v1alpha1.RelationData{strconv.FormatInt(remoteID, 10): {"app": "setting"}, "999": {"not": "this relation"}}}}
	if err := c.Create(ctx, ad); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the consumer reads the offered application's data", func() bool {
		rd := remoteData(c, consumer, rel.Name)
		return rd != nil && rd.Spec.Application == "pg" && rd.Spec.Source == offering+"/db" && rd.Spec.RelationID == id &&
			len(rd.Spec.Units) == 1 && rd.Spec.Units["pg/0"].Data["host"] == "10.0.0.1" && rd.Spec.AppData["app"] == "setting" && len(rd.Spec.AppData) == 1
	})
	eventually(t, "the offering side reads the consumer's data", func() bool {
		rd := remoteData(c, offering, mirrorName)
		return rd != nil && rd.Spec.Application == mirror.Spec.Alias && rd.Spec.Source == consumer+"/client" && rd.Spec.RelationID == remoteID &&
			len(rd.Spec.Units) == 1 && rd.Spec.Units[mirror.Spec.Alias+"/0"].Data["who"] == "client-0"
	})
	// A change travels, and leaving the relation takes the unit away.
	unitInRelation(t, c, offering, "db", 0, remoteID, true, map[string]string{"host": "10.0.0.2"})
	eventually(t, "changed data", func() bool {
		rd := remoteData(c, consumer, rel.Name)
		return rd != nil && rd.Spec.Units["pg/0"].Data["host"] == "10.0.0.2"
	})
	unitInRelation(t, c, offering, "db", 0, remoteID, false, nil)
	eventually(t, "a unit that left is no longer shown", func() bool {
		rd := remoteData(c, consumer, rel.Name)
		return rd != nil && len(rd.Spec.Units) == 0
	})
	unitInRelation(t, c, offering, "db", 0, remoteID, true, map[string]string{"host": "10.0.0.3"})
	eventually(t, "the unit is back", func() bool {
		rd := remoteData(c, consumer, rel.Name)
		return rd != nil && rd.Spec.Units["pg/0"].Data["host"] == "10.0.0.3"
	})

	// Removal waits for the units of both sides, each with its own id, in its own namespace.
	createPod(t, c, offering, "db", 0, true)
	createPod(t, c, consumer, "client", 0, true)
	if err := c.Delete(ctx, relationOf(c, consumer, rel.Name)); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the mirror is deleted at once", func() bool { return gone(c, offering, mirrorName, &v1alpha1.Relation{}) })
	eventually(t, "Removing names the units of both namespaces", func() bool {
		r := relationOf(c, consumer, rel.Name)
		if r == nil {
			return false
		}
		cond := meta.FindStatusCondition(r.Status.Conditions, v1alpha1.RelationRemoving)
		return cond != nil && strings.Contains(cond.Message, "client/0") && strings.Contains(cond.Message, offering+"/db/0")
	})
	unitInRelation(t, c, consumer, "client", 0, id, false, nil)
	consistently(t, "the relation went while a unit of the offering side was still in it", func() bool { return exists(c, consumer, rel.Name, &v1alpha1.Relation{}) })
	unitInRelation(t, c, offering, "db", 0, remoteID, false, nil)
	eventually(t, "the relation is gone", func() bool { return gone(c, consumer, rel.Name, &v1alpha1.Relation{}) })
	eventually(t, "the offer shows no connection", func() bool {
		var o v1alpha1.Offer
		return get(c, offering, "pg", &o) == nil && len(o.Status.Connections) == 0
	})
	_ = offer
}

func controllerutilHasFinalizer(o *v1alpha1.Offer) bool {
	for _, f := range o.Finalizers {
		if f == v1alpha1.OfferFinalizer {
			return true
		}
	}
	return false
}

// counter reads the relation id counter of a namespace.
func counter(t *testing.T, c client.Client, ns string) int64 {
	t.Helper()
	var cm corev1.ConfigMap
	if err := get(c, ns, v1alpha1.ModelConfigMap, &cm); err != nil {
		t.Fatal(err)
	}
	n, _ := strconv.ParseInt(cm.Data[operator.RelationIDKey], 10, 64)
	return n
}

func TestOfferedRelationValidation(t *testing.T) {
	t.Parallel()
	c := startOperator(t)
	addRelationCharms()
	offering := newNamespace(t, c, true)
	consumer := newNamespace(t, c, true)
	plain := newNamespace(t, c, false)
	newApp(t, c, offering, "db", digestProvider, 1)
	newApp(t, c, consumer, "client", digestClient, 1)
	newApp(t, c, consumer, "client2", digestClient, 1)
	newApp(t, c, consumer, "pg", digestClient, 1) // an application called like the offer
	for _, a := range []struct{ ns, name string }{{offering, "db"}, {consumer, "client"}, {consumer, "client2"}, {consumer, "pg"}} {
		eventually(t, a.name+" charm resolved", func() bool { r := ready(c, a.ns, a.name); return r != nil && r.Status.Charm != nil })
	}
	newOffer(t, c, offering, "pg", "db", []string{"database", "limited"}, consumer)
	newOffer(t, c, offering, "bad", "db", []string{"nope"}, consumer)

	cases := []struct {
		name, reason string
		rel          *v1alpha1.Relation
	}{
		{"no-offer", "OfferMissing", consume(t, c, "no-offer", consumer, "client", "db", offering, "db", "database", "", "")},
		{"unknown-offer", "OfferNotFound", consume(t, c, "unknown-offer", consumer, "client", "db", offering, "db", "database", "ghost", "")},
		{"not-offered", "OfferEndpoint", consume(t, c, "not-offered", consumer, "client", "db", offering, "db", "certs", "pg", "")},
		{"alias-is-an-app", "InvalidAlias", consume(t, c, "alias-is-an-app", consumer, "client", "db", offering, "db", "database", "pg", "")},
		{"bad-alias", "InvalidAlias", consume(t, c, "bad-alias", consumer, "client", "db", offering, "db", "database", "pg", "No_Good")},
		{"not-a-model", "NotAModel", consume(t, c, "not-a-model", consumer, "client", "db", plain, "db", "database", "pg", "x")},
	}
	for _, tc := range cases {
		waitInvalid(t, c, consumer, tc.name, tc.reason)
		if cond := readyCondition(c, consumer, tc.name); cond == nil || cond.Status != metav1.ConditionFalse {
			t.Errorf("%s: ready %+v", tc.name, cond)
		}
		if r := relationOf(c, consumer, tc.name); r.Status.RemoteID != 0 {
			t.Errorf("%s: invalid relation got a remote id", tc.name)
		}
	}
	// Nothing was mirrored for any of them.
	var rels v1alpha1.RelationList
	if err := c.List(context.Background(), &rels, client.InNamespace(offering)); err != nil {
		t.Fatal(err)
	}
	for _, r := range rels.Items {
		if r.Labels[v1alpha1.RemoteLabel] == "true" {
			t.Errorf("mirror %s of an invalid relation", r.Name)
		}
	}

	// (An older invalid relation holds back a newer one between the same endpoints, so the rejected ones go first.)
	for _, tc := range cases {
		if err := c.Delete(context.Background(), tc.rel); err != nil {
			t.Fatal(err)
		}
		tc := tc
		eventually(t, tc.name+" removed", func() bool { return gone(c, consumer, tc.name, &v1alpha1.Relation{}) })
	}
	// An alias chosen by the consumer makes the offer's name usable; the offered endpoint's limit counts across namespaces.
	waitValid(t, c, consumer, consume(t, c, "alias-ok", consumer, "client", "db", offering, "db", "database", "pg", "pg-remote").Name)
	waitValid(t, c, consumer, consume(t, c, "lim1", consumer, "client", "lim", offering, "db", "limited", "pg", "lim-remote").Name)
	consume(t, c, "lim2", consumer, "client2", "lim", offering, "db", "limited", "pg", "lim2-remote")
	waitInvalid(t, c, consumer, "lim2", "LimitExceeded")
	// Two relations cannot take the same alias.
	consume(t, c, "dup-alias", consumer, "client2", "db", offering, "db", "database", "pg", "pg-remote")
	waitInvalid(t, c, consumer, "dup-alias", "InvalidAlias")
	// An Offer that turns up later makes a waiting relation valid.
	late := consume(t, c, "late", consumer, "client2", "db2", offering, "db", "database", "later", "")
	waitInvalid(t, c, consumer, late.Name, "OfferNotFound")
	newOffer(t, c, offering, "later", "db", []string{"database"}, consumer)
	waitValid(t, c, consumer, late.Name)
}

func TestOfferStatusAndRemoval(t *testing.T) {
	t.Parallel()
	c := startOperator(t)
	addRelationCharms()
	ctx := context.Background()
	offering := newNamespace(t, c, true)
	consumer := newNamespace(t, c, true)
	newApp(t, c, offering, "db", digestProvider, 1)
	newApp(t, c, consumer, "client", digestClient, 1)
	for _, a := range []struct{ ns, name string }{{offering, "db"}, {consumer, "client"}} {
		eventually(t, a.name+" charm resolved", func() bool { r := ready(c, a.ns, a.name); return r != nil && r.Status.Charm != nil })
	}
	// Offers that cannot work say why.
	newOffer(t, c, offering, "ghost", "nothing", []string{"database"})
	newOffer(t, c, offering, "wrong", "db", []string{"nope"})
	newOffer(t, c, offering, "peer", "db", []string{"cluster"})
	for name, reason := range map[string]string{"ghost": "ApplicationNotFound", "wrong": "EndpointNotFound", "peer": "PeerEndpoint"} {
		eventually(t, name+" not ready: "+reason, func() bool {
			var o v1alpha1.Offer
			cond := (*metav1.Condition)(nil)
			if get(c, offering, name, &o) == nil {
				cond = meta.FindStatusCondition(o.Status.Conditions, v1alpha1.OfferReady)
			}
			return cond != nil && cond.Status == metav1.ConditionFalse && cond.Reason == reason
		})
	}
	// "*" lets every model in.
	newOffer(t, c, offering, "open", "db", []string{"database"}, "*")
	eventually(t, "the access ConfigMap says every model may consume", func() bool {
		var cm corev1.ConfigMap
		if get(c, v1alpha1.SystemNamespace, v1alpha1.OfferAccessConfigMap, &cm) != nil {
			return false
		}
		_, ok := cm.Data[operator.OfferAccessKey(offering, operator.OfferAccessKeyAll)]
		return ok
	})

	// Deleting an offer removes its connections first.
	rel := consume(t, c, "via-open", consumer, "client", "db", offering, "db", "database", "open", "")
	waitValid(t, c, consumer, rel.Name)
	id := relationOf(c, consumer, rel.Name).Status.ID
	createPod(t, c, consumer, "client", 0, true)
	unitInRelation(t, c, consumer, "client", 0, id, true, map[string]string{"x": "y"})
	if err := c.Delete(ctx, &v1alpha1.Offer{ObjectMeta: metav1.ObjectMeta{Namespace: offering, Name: "open"}}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the relation is being removed with its offer", func() bool {
		r := relationOf(c, consumer, rel.Name)
		return r != nil && r.DeletionTimestamp != nil
	})
	consistently(t, "the offer went while a unit was still in the relation", func() bool { return exists(c, offering, "open", &v1alpha1.Offer{}) })
	unitInRelation(t, c, consumer, "client", 0, id, false, nil)
	eventually(t, "the relation and then the offer are gone", func() bool {
		return gone(c, consumer, rel.Name, &v1alpha1.Relation{}) && gone(c, offering, "open", &v1alpha1.Offer{})
	})
	eventually(t, "the access ConfigMap forgets the offer", func() bool {
		var cm corev1.ConfigMap
		if get(c, v1alpha1.SystemNamespace, v1alpha1.OfferAccessConfigMap, &cm) != nil {
			return false
		}
		_, ok := cm.Data[operator.OfferAccessKey(offering, operator.OfferAccessKeyAll)]
		return !ok
	})
}

func newSecret(t *testing.T, c client.Client, ns, name, xid, owner string, data map[string][]byte, annotations map[string]string, revision string) {
	t.Helper()
	labels := map[string]string{v1alpha1.SecretLabel: xid, v1alpha1.AppLabel: owner}
	if revision != "" {
		labels[v1alpha1.SecretRevisionLabel] = revision
	}
	s := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, Labels: labels, Annotations: annotations}, Data: data}
	if err := c.Create(context.Background(), s); err != nil {
		t.Fatal(err)
	}
}

func secretOf(c client.Client, ns, name string) *corev1.Secret {
	var s corev1.Secret
	if get(c, ns, name, &s) != nil {
		return nil
	}
	return &s
}

// Secrets granted to the application on the other side of a relation across namespaces are copied over, and kept in step.
func TestOfferedRelationSecrets(t *testing.T) {
	t.Parallel()
	c := startOperator(t)
	addRelationCharms()
	ctx := context.Background()
	offering := newNamespace(t, c, true)
	consumer := newNamespace(t, c, true)
	newApp(t, c, offering, "db", digestProvider, 1)
	newApp(t, c, consumer, "client", digestClient, 1)
	for _, a := range []struct{ ns, name string }{{offering, "db"}, {consumer, "client"}} {
		eventually(t, a.name+" charm resolved", func() bool { r := ready(c, a.ns, a.name); return r != nil && r.Status.Charm != nil })
	}
	newOffer(t, c, offering, "pg", "db", []string{"database"}, consumer)
	rel := consume(t, c, "client.db-pg", consumer, "client", "db", offering, "db", "database", "pg", "")
	var id, remoteID int64
	eventually(t, "ids", func() bool {
		r := relationOf(c, consumer, rel.Name)
		id, remoteID = r.Status.ID, r.Status.RemoteID
		return id != 0 && remoteID != 0
	})
	mirrorName := "remote." + consumer + "." + rel.Name
	var alias string
	eventually(t, "the mirror", func() bool {
		m := relationOf(c, offering, mirrorName)
		if m == nil {
			return false
		}
		alias = m.Spec.Alias
		return true
	})

	grants := `[{"application":"` + alias + `"}]`
	meta1 := map[string]string{v1alpha1.SecretOwnerAnnotation: "application:db", v1alpha1.SecretLatestAnnotation: "1", v1alpha1.SecretGrantsAnnotation: grants}
	newSecret(t, c, offering, "jk-secret-abc", "abc", "db", nil, meta1, "")
	newSecret(t, c, offering, "jk-secret-abc-1", "abc", "db", map[string][]byte{"token": []byte("one")}, nil, "1")
	// A secret that is not granted to this relation is not copied.
	newSecret(t, c, offering, "jk-secret-private", "private", "db", nil, map[string]string{v1alpha1.SecretGrantsAnnotation: `[{"application":"someone-else"}]`, v1alpha1.SecretLatestAnnotation: "1"}, "")
	newSecret(t, c, offering, "jk-secret-private-1", "private", "db", map[string][]byte{"token": []byte("no")}, nil, "1")

	eventually(t, "the secret is copied to the consumer's namespace, granted to its application", func() bool {
		m, r := secretOf(c, consumer, "jk-secret-abc"), secretOf(c, consumer, "jk-secret-abc-1")
		return m != nil && r != nil && string(r.Data["token"]) == "one" && m.Labels[v1alpha1.AppLabel] == "pg" &&
			m.Annotations[v1alpha1.SecretGrantsAnnotation] == `[{"application":"client"}]` && m.Annotations[v1alpha1.SecretOwnerAnnotation] == "application:pg"
	})
	if secretOf(c, consumer, "jk-secret-private") != nil {
		t.Fatal("an ungranted secret was copied")
	}
	// The consumer's application may read the copies by name, like any granted secret, and the data entry says the secret is there.
	eventually(t, "a Role lets the consumer's application read the copy", func() bool {
		var role rbacv1.Role
		if get(c, consumer, operator.SecretsRoleName("client"), &role) != nil {
			return false
		}
		for _, r := range role.Rules {
			if strings.Join(r.ResourceNames, ",") == "jk-secret-abc,jk-secret-abc-1" {
				return true
			}
		}
		return false
	})
	eventually(t, "the remote data lists the secret", func() bool {
		rd := remoteData(c, consumer, rel.Name)
		return rd != nil && rd.Spec.Secrets["abc"] == 1 && len(rd.Spec.Secrets) == 1
	})

	// A new revision, and then the end of the old one.
	s := secretOf(c, offering, "jk-secret-abc")
	s.Annotations[v1alpha1.SecretLatestAnnotation] = "2"
	if err := c.Update(ctx, s); err != nil {
		t.Fatal(err)
	}
	newSecret(t, c, offering, "jk-secret-abc-2", "abc", "db", map[string][]byte{"token": []byte("two")}, nil, "2")
	eventually(t, "revision 2 is copied", func() bool {
		r := secretOf(c, consumer, "jk-secret-abc-2")
		m := secretOf(c, consumer, "jk-secret-abc")
		rd := remoteData(c, consumer, rel.Name)
		return r != nil && string(r.Data["token"]) == "two" && m.Annotations[v1alpha1.SecretLatestAnnotation] == "2" && rd.Spec.Secrets["abc"] == 2
	})
	if err := c.Delete(ctx, secretOf(c, offering, "jk-secret-abc-1")); err != nil {
		t.Fatal(err)
	}
	eventually(t, "revision 1 is gone from the copy", func() bool { return secretOf(c, consumer, "jk-secret-abc-1") == nil })

	// The other direction: what the consumer's application grants to the offered one.
	back := `[{"application":"pg"}]`
	newSecret(t, c, consumer, "jk-secret-mine", "mine", "client", nil, map[string]string{v1alpha1.SecretOwnerAnnotation: "application:client", v1alpha1.SecretLatestAnnotation: "1", v1alpha1.SecretGrantsAnnotation: back}, "")
	newSecret(t, c, consumer, "jk-secret-mine-1", "mine", "client", map[string][]byte{"k": []byte("v")}, nil, "1")
	eventually(t, "the consumer's secret is copied to the offering namespace, granted to db", func() bool {
		m, r := secretOf(c, offering, "jk-secret-mine"), secretOf(c, offering, "jk-secret-mine-1")
		rd := remoteData(c, offering, mirrorName)
		return m != nil && r != nil && string(r.Data["k"]) == "v" && m.Labels[v1alpha1.AppLabel] == alias &&
			m.Annotations[v1alpha1.SecretGrantsAnnotation] == `[{"application":"db"}]` && rd != nil && rd.Spec.Secrets["mine"] == 1
	})
	// The copy is not copied back, and is not mistaken for the offering side's own secret.
	consistently(t, "a copy was copied back", func() bool { return secretOf(c, consumer, "jk-secret-private") == nil })

	// Revoking the grant takes the copies away.
	s = secretOf(c, offering, "jk-secret-abc")
	s.Annotations[v1alpha1.SecretGrantsAnnotation] = `[]`
	if err := c.Update(ctx, s); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the revoked secret is gone from the consumer's namespace", func() bool {
		return secretOf(c, consumer, "jk-secret-abc") == nil && secretOf(c, consumer, "jk-secret-abc-2") == nil
	})
	if secretOf(c, offering, "jk-secret-abc") == nil || secretOf(c, offering, "jk-secret-abc-2") == nil {
		t.Fatal("the original was touched")
	}

	// Ending the relation removes the copies on both sides.
	if err := c.Delete(ctx, relationOf(c, consumer, rel.Name)); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the relation is gone", func() bool { return gone(c, consumer, rel.Name, &v1alpha1.Relation{}) })
	eventually(t, "the copies are gone from the offering namespace", func() bool {
		return secretOf(c, offering, "jk-secret-mine") == nil && secretOf(c, offering, "jk-secret-mine-1") == nil
	})
	if secretOf(c, consumer, "jk-secret-mine") == nil {
		t.Fatal("the consumer's own secret was removed")
	}
}
