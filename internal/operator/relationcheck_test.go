package operator

import (
	"encoding/json"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/luci1900/jk/api/v1alpha1"
)

const (
	dbMeta     = `{"provides":{"database":{"interface":"pg","limit":1},"extra":"other"},"requires":{"certs":{"interface":"tls"}},"peers":{"cluster":{"interface":"pgp"}}}`
	clientMeta = `{"requires":{"db":{"interface":"pg"},"db2":{"interface":"pg"},"bad":{"interface":"other"}},"provides":{"out":{"interface":"pg"}}}`
)

func ep(app, name string) v1alpha1.EndpointRef {
	return v1alpha1.EndpointRef{Namespace: "ns", Application: app, Endpoint: name}
}

func apps(t *testing.T) map[string]relationApp {
	return map[string]relationApp{"ns/db": {mustMD(t, dbMeta)}, "ns/client": {mustMD(t, clientMeta)}, "ns/client2": {mustMD(t, clientMeta)}, "ns/pending": {}}
}

func TestValidateRelation(t *testing.T) {
	t0 := time.Unix(1000, 0)
	tests := []struct {
		name    string
		eps     []v1alpha1.EndpointRef
		peerLbl bool
		id      int64
		created time.Time
		others  []relationRef
		want    string
	}{
		{"valid", []v1alpha1.EndpointRef{ep("db", "database"), ep("client", "db")}, false, 0, t0, nil, ""},
		{"valid reversed", []v1alpha1.EndpointRef{ep("client", "db"), ep("db", "database")}, false, 0, t0, nil, ""},
		{"shorthand interface", []v1alpha1.EndpointRef{ep("db", "extra"), ep("client", "bad")}, false, 0, t0, nil, ""},
		{"no endpoints", nil, false, 0, t0, nil, ReasonBadEndpoints},
		{"three endpoints", []v1alpha1.EndpointRef{ep("db", "database"), ep("client", "db"), ep("client2", "db")}, false, 0, t0, nil, ReasonBadEndpoints},
		{"other namespace", []v1alpha1.EndpointRef{ep("db", "database"), {Namespace: "x", Application: "client", Endpoint: "db"}}, false, 0, t0, nil, ReasonOfferMissing},
		{"missing app", []v1alpha1.EndpointRef{ep("db", "database"), ep("nope", "db")}, false, 0, t0, nil, ReasonApplicationAbsent},
		{"unresolved charm", []v1alpha1.EndpointRef{ep("db", "database"), ep("pending", "db")}, false, 0, t0, nil, ReasonCharmPending},
		{"missing endpoint", []v1alpha1.EndpointRef{ep("db", "database"), ep("client", "zzz")}, false, 0, t0, nil, ReasonEndpointNotFound},
		{"same app", []v1alpha1.EndpointRef{ep("client", "out"), ep("client", "db")}, false, 0, t0, nil, ReasonSameApplication},
		{"both provide", []v1alpha1.EndpointRef{ep("db", "database"), ep("client", "out")}, false, 0, t0, nil, ReasonIncompatible},
		{"both require", []v1alpha1.EndpointRef{ep("db", "certs"), ep("client", "db")}, false, 0, t0, nil, ReasonIncompatible},
		{"peer endpoint in a pair", []v1alpha1.EndpointRef{ep("db", "cluster"), ep("client", "db")}, false, 0, t0, nil, ReasonIncompatible},
		{"interface mismatch", []v1alpha1.EndpointRef{ep("db", "database"), ep("client", "bad")}, false, 0, t0, nil, ReasonInterfaceMismatch},
		{"peer by operator", []v1alpha1.EndpointRef{ep("db", "cluster")}, true, 0, t0, nil, ""},
		{"peer by user", []v1alpha1.EndpointRef{ep("db", "cluster")}, false, 0, t0, nil, ReasonPeerRelation},
		{"single non-peer", []v1alpha1.EndpointRef{ep("db", "database")}, true, 0, t0, nil, ReasonBadEndpoints},
		{"duplicate of older", []v1alpha1.EndpointRef{ep("db", "database"), ep("client", "db")}, false, 0, t0,
			[]relationRef{{Name: "first", Endpoints: []v1alpha1.EndpointRef{ep("client", "db"), ep("db", "database")}, Created: t0.Add(-time.Second)}}, ReasonDuplicate},
		{"older duplicate wins", []v1alpha1.EndpointRef{ep("db", "database"), ep("client", "db")}, false, 0, t0,
			[]relationRef{{Name: "later", Endpoints: []v1alpha1.EndpointRef{ep("db", "database"), ep("client", "db")}, Created: t0.Add(time.Second)}}, ""},
		{"duplicate with id outranks older", []v1alpha1.EndpointRef{ep("db", "database"), ep("client", "db")}, false, 0, t0,
			[]relationRef{{Name: "x", ID: 3, Endpoints: []v1alpha1.EndpointRef{ep("db", "database"), ep("client", "db")}, Created: t0.Add(time.Hour)}}, ReasonDuplicate},
		{"duplicate same age by name", []v1alpha1.EndpointRef{ep("db", "database"), ep("client", "db")}, false, 0, t0,
			[]relationRef{{Name: "a", Endpoints: []v1alpha1.EndpointRef{ep("db", "database"), ep("client", "db")}, Created: t0}}, ReasonDuplicate},
		{"limit reached", []v1alpha1.EndpointRef{ep("db", "database"), ep("client2", "db")}, false, 0, t0,
			[]relationRef{{Name: "r1", ID: 1, Endpoints: []v1alpha1.EndpointRef{ep("db", "database"), ep("client", "db")}}}, ReasonLimitExceeded},
		{"limit ignores relations without an id", []v1alpha1.EndpointRef{ep("db", "database"), ep("client2", "db")}, false, 0, t0,
			[]relationRef{{Name: "r1", Endpoints: []v1alpha1.EndpointRef{ep("db", "database"), ep("client", "db")}}}, ""},
		{"established relation is never held back", []v1alpha1.EndpointRef{ep("db", "database"), ep("client2", "db")}, false, 5, t0,
			[]relationRef{{Name: "r1", ID: 1, Endpoints: []v1alpha1.EndpointRef{ep("db", "database"), ep("client", "db")}}}, ""},
		{"unlimited endpoint", []v1alpha1.EndpointRef{ep("db", "database"), ep("client", "db2")}, false, 0, t0,
			[]relationRef{{Name: "r1", ID: 1, Endpoints: []v1alpha1.EndpointRef{ep("client", "db"), ep("db", "certs")}}}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := ValidateRelation("ns", tt.eps, tt.peerLbl, "me", tt.id, tt.created, apps(t), tt.others, nil)
			got := ""
			if p != nil {
				got = p.Reason
				if p.Message == "" || p.Error() == "" {
					t.Error("empty message")
				}
			}
			if got != tt.want {
				t.Errorf("reason %q (%v), want %q", got, p, tt.want)
			}
		})
	}
}

func TestLimitValue(t *testing.T) {
	for in, want := range map[string]int{`{"interface":"a","limit":2}`: 2, `{"interface":"a","limit":"none"}`: 0, `{"interface":"a","limit":0}`: 0, `{"interface":"a","limit":1.5}`: 0, `{"interface":"a"}`: 0, `"a"`: 0} {
		var e relationEndpoint
		if err := json.Unmarshal([]byte(in), &e); err != nil || int(e.Limit) != want || e.Interface != "a" {
			t.Errorf("%s: %+v %v", in, e, err)
		}
	}
	var e relationEndpoint
	if json.Unmarshal([]byte(`[1]`), &e) == nil {
		t.Error("a list is not an endpoint")
	}
}

func TestUnitsInScope(t *testing.T) {
	ud := func(name string, id string, in bool) v1alpha1.UnitData {
		u := v1alpha1.UnitData{ObjectMeta: metav1.ObjectMeta{Name: name}}
		if id != "" {
			u.Spec.Relations = map[string]v1alpha1.UnitRelationState{id: {InScope: in}}
		}
		return u
	}
	units := []v1alpha1.UnitData{
		ud("db-1", "7", true), ud("db-0", "7", true), ud("db-2", "7", false), ud("db-3", "", false),
		ud("client-0", "7", true), ud("client-1", "8", true), ud("other-0", "7", true), ud("client-2", "7", true),
	}
	pods := map[string]bool{"db-0": true, "db-1": true, "db-2": true, "client-0": true, "client-1": true, "other-0": true}
	got := UnitsInScope(7, []string{"db", "client"}, units, func(n string) bool { return pods[n] })
	want := []string{"client/0", "db/0", "db/1"}
	if len(got) != len(want) {
		t.Fatalf("%v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%v, want %v", got, want)
		}
	}
}

func TestGrantRelationIDAndLiveGrants(t *testing.T) {
	for in, want := range map[string]int64{"7": 7, "database:12": 12, "a b:c:3": 3, "": 0, "x": 0, "0": 0} {
		if got, ok := GrantRelationID(in); got != want || ok != (want != 0) {
			t.Errorf("%q: %d %v", in, got, ok)
		}
	}
	grants := `[{"application":"web","relation":"database:7"},{"application":"old","relation":"3"},{"application":"all"}]`
	secrets := []corev1.Secret{
		{ObjectMeta: metav1.ObjectMeta{Name: "jk-secret-x", Labels: map[string]string{v1alpha1.SecretLabel: "x", v1alpha1.AppLabel: "pg"}, Annotations: map[string]string{v1alpha1.SecretGrantsAnnotation: grants}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "jk-secret-x-1", Labels: map[string]string{v1alpha1.SecretLabel: "x", v1alpha1.AppLabel: "pg"}}},
	}
	live := func(id int64) bool { return id == 7 }
	for app, want := range map[string]int{"web": 2, "old": 0, "all": 2, "pg": 0} {
		_, granted := SecretNames(app, secrets, live)
		if len(granted) != want {
			t.Errorf("%s granted %v, want %d", app, granted, want)
		}
	}
	if _, granted := SecretNames("old", secrets, nil); len(granted) != 2 {
		t.Errorf("without a relation filter: %v", granted)
	}
}

func TestValidateRelationAcrossNamespaces(t *testing.T) {
	t0 := time.Unix(1000, 0)
	remote := func(app, name string) v1alpha1.EndpointRef {
		return v1alpha1.EndpointRef{Namespace: "far", Application: app, Endpoint: name}
	}
	offer := &v1alpha1.Offer{ObjectMeta: metav1.ObjectMeta{Namespace: "far", Name: "pg"}, Spec: v1alpha1.OfferSpec{Application: "db", Endpoints: []string{"database", "extra"}}}
	known := func(t *testing.T) map[string]relationApp {
		return map[string]relationApp{
			"ns/client": {mustMD(t, clientMeta)}, "ns/client2": {mustMD(t, clientMeta)}, "far/db": {mustMD(t, dbMeta)}, "far/client": {mustMD(t, clientMeta)},
		}
	}
	ok := func() *crossInfo {
		return &crossInfo{OfferName: "pg", Offer: offer, Alias: "pg", LocalApps: map[string]bool{"client": true, "client2": true}, AliasUsed: map[string]bool{}, RemoteIsModel: true}
	}
	eps := []v1alpha1.EndpointRef{ep("client", "db"), remote("db", "database")}
	tests := []struct {
		name   string
		eps    []v1alpha1.EndpointRef
		cross  func(*crossInfo) *crossInfo
		others []relationRef
		want   string
	}{
		{"valid", eps, nil, nil, ""},
		{"valid, the offered endpoint first", []v1alpha1.EndpointRef{remote("db", "database"), ep("client", "db")}, nil, nil, ""},
		{"no offer named", eps, func(c *crossInfo) *crossInfo { c.OfferName = ""; return c }, nil, ReasonOfferMissing},
		{"no cross info", eps, func(*crossInfo) *crossInfo { return nil }, nil, ReasonOfferMissing},
		{"offering namespace is not a model", eps, func(c *crossInfo) *crossInfo { c.RemoteIsModel = false; return c }, nil, ReasonNotAModel},
		{"offer not found", eps, func(c *crossInfo) *crossInfo { c.Offer = nil; return c }, nil, ReasonOfferNotFound},
		{"endpoint not offered", []v1alpha1.EndpointRef{ep("client", "db"), remote("db", "certs")}, nil, nil, ReasonOfferEndpoint},
		{"other application than offered", []v1alpha1.EndpointRef{ep("client", "db"), remote("client", "db")}, nil, nil, ReasonOfferEndpoint},
		{"alias is a local application", eps, func(c *crossInfo) *crossInfo { c.Alias = "client2"; return c }, nil, ReasonAlias},
		{"alias used by another relation", eps, func(c *crossInfo) *crossInfo { c.AliasUsed["pg"] = true; return c }, nil, ReasonAlias},
		{"alias is not a name", eps, func(c *crossInfo) *crossInfo { c.Alias = "Pg_1"; return c }, nil, ReasonAlias},
		{"two other namespaces", []v1alpha1.EndpointRef{remote("db", "database"), {Namespace: "third", Application: "client", Endpoint: "db"}}, nil, nil, ReasonCrossNamespace},
		{"both in the offering namespace", []v1alpha1.EndpointRef{remote("db", "database"), remote("client", "db")}, nil, nil, ReasonCrossNamespace},
		{"interface mismatch", []v1alpha1.EndpointRef{ep("client", "bad"), remote("db", "database")}, nil, nil, ReasonInterfaceMismatch},
		{"the offered endpoint's limit counts relations of other namespaces", eps, nil,
			[]relationRef{{Name: "far/mirror", ID: 4, Endpoints: []v1alpha1.EndpointRef{remote("db", "database"), remote("client", "db")}}}, ReasonLimitExceeded},
		{"a duplicate in the same namespaces", eps, nil,
			[]relationRef{{Name: "first", Endpoints: eps, Created: t0.Add(-time.Second)}}, ReasonDuplicate},
		{"same application names in other namespaces are not duplicates", []v1alpha1.EndpointRef{ep("client", "db"), remote("db", "database")}, nil,
			[]relationRef{{Name: "x", Endpoints: []v1alpha1.EndpointRef{{Namespace: "other", Application: "client", Endpoint: "db"}, remote("db", "database")}}}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cross := ok()
			if tt.cross != nil {
				cross = tt.cross(cross)
			}
			p := ValidateRelation("ns", tt.eps, false, "me", 0, t0, known(t), tt.others, cross)
			got := ""
			if p != nil {
				got = p.Reason
			}
			if got != tt.want {
				t.Fatalf("reason %q (%v), want %q", got, p, tt.want)
			}
		})
	}
}
