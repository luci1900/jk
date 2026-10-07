package operator

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/luci1900/jk/api/v1alpha1"
)

func TestOfferAccess(t *testing.T) {
	offer := func(ns, name string, deleting bool, allowed ...string) v1alpha1.Offer {
		o := v1alpha1.Offer{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}, Spec: v1alpha1.OfferSpec{AllowedModels: allowed}}
		if deleting {
			now := metav1.Now()
			o.DeletionTimestamp = &now
		}
		return o
	}
	got := OfferAccess([]v1alpha1.Offer{
		offer("db", "pg", false, "web", "cos"),
		offer("db", "other", false, "web"),
		offer("cos", "prom", false, "*"),
		offer("gone", "x", true, "web"),
		offer("empty", "none", false),
		offer("odd", "blank", false, ""),
	})
	want := map[string]string{"db.web": "", "db.cos": "", "cos._all": ""}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%v", got)
	}
	if got := OfferAccess(nil); len(got) != 0 {
		t.Fatalf("%v", got)
	}
	// Namespace names cannot hold a dot or an underscore, so keys are never ambiguous.
	if OfferAccessKey("a", "b") != "a.b" || OfferAccessKey("a", OfferAccessKeyAll) != "a._all" {
		t.Fatal("key format")
	}
}

func TestRemoteSettings(t *testing.T) {
	unit := func(name string, id string, inScope bool, data map[string]string) v1alpha1.UnitData {
		return v1alpha1.UnitData{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: v1alpha1.UnitDataSpec{
			Relations: map[string]v1alpha1.UnitRelationState{id: {InScope: inScope, Data: data}, "99": {InScope: true, Data: map[string]string{"other": "relation"}}},
		}}
	}
	units := []v1alpha1.UnitData{
		unit("db-0", "7", true, map[string]string{"k": "v0"}),
		unit("db-1", "7", false, map[string]string{"k": "left"}), // out of scope: not shown
		unit("db-2", "8", true, map[string]string{"k": "other id"}),
		unit("db-10", "7", true, map[string]string{"k": "v10"}),
		unit("client-0", "7", true, map[string]string{"k": "other app"}),
		unit("db-x", "7", true, map[string]string{"k": "not a unit"}),
	}
	ad := &v1alpha1.AppData{Spec: v1alpha1.AppDataSpec{Relations: map[string]v1alpha1.RelationData{"7": {"app": "data"}, "8": {"x": "y"}}}}
	got, app, err := RemoteSettings(units, "db", "pg", 7, ad)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]v1alpha1.RemoteUnit{"pg/0": {Data: map[string]string{"k": "v0"}}, "pg/10": {Data: map[string]string{"k": "v10"}}}
	if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(app, v1alpha1.RelationData{"app": "data"}) {
		t.Fatalf("%v %v", got, app)
	}
	// Nothing in the relation: nil, so the stored object does not change with every empty map.
	got, app, _ = RemoteSettings(units, "db", "pg", 55, nil)
	if got != nil || app != nil {
		t.Fatalf("%v %v", got, app)
	}
	// The copy does not alias the source.
	got, _, _ = RemoteSettings(units, "db", "pg", 7, nil)
	got["pg/0"].Data["k"] = "changed"
	if units[0].Spec.Relations["7"].Data["k"] != "v0" {
		t.Fatal("settings aliased")
	}
}

func TestMirrorNames(t *testing.T) {
	rel := &v1alpha1.Relation{ObjectMeta: metav1.ObjectMeta{Namespace: "web", Name: "client.db-pg.database"}}
	if mirrorName(rel) != "remote.web.client.db-pg.database" {
		t.Fatal(mirrorName(rel))
	}
	a := mirrorAlias(rel)
	if a != mirrorAlias(rel) || len(a) != len("remote-")+8 {
		t.Fatal(a)
	}
	rel2 := rel.DeepCopy()
	rel2.Namespace = "web2"
	if mirrorAlias(rel2) == a || !aliasRule.MatchString(a) {
		t.Fatalf("%s %s", a, mirrorAlias(rel2))
	}
	m := &v1alpha1.Relation{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{v1alpha1.RemoteOfAnnotation: "web/client.db"}}}
	if ns, name, ok := remoteOf(m); !ok || ns != "web" || name != "client.db" {
		t.Fatalf("%s %s %v", ns, name, ok)
	}
	if _, _, ok := remoteOf(&v1alpha1.Relation{}); ok {
		t.Fatal("no annotation")
	}
	if aliasOf(&v1alpha1.Relation{Spec: v1alpha1.RelationSpec{Offer: "pg"}}) != "pg" || aliasOf(&v1alpha1.Relation{Spec: v1alpha1.RelationSpec{Offer: "pg", Alias: "db"}}) != "db" {
		t.Fatal("alias")
	}
}

func secretObjs(xid, owner, grants string, latest string, revisions ...string) []corev1.Secret {
	meta := corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "far", Name: v1alpha1.SecretMetadataName(xid),
		Labels:      map[string]string{v1alpha1.SecretLabel: xid, v1alpha1.AppLabel: owner},
		Annotations: map[string]string{v1alpha1.SecretOwnerAnnotation: "application:" + owner, v1alpha1.SecretLatestAnnotation: latest, v1alpha1.SecretGrantsAnnotation: grants, "jk.luci1900.github.io/secret-label": "mine"}}}
	out := []corev1.Secret{meta}
	for _, r := range revisions {
		out = append(out, corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "far", Name: v1alpha1.SecretNamePrefix + xid + "-" + r,
			Labels: map[string]string{v1alpha1.SecretLabel: xid, v1alpha1.AppLabel: owner, v1alpha1.SecretRevisionLabel: r}}, Data: map[string][]byte{"token": []byte("t" + r)}})
	}
	return out
}

func TestMirrorSecrets(t *testing.T) {
	p := MirrorParams{SrcApp: "db", Grantee: "remote-1", RelationID: 5, DstNS: "web", DstApp: "client", OwnerAlias: "pg", MirrorID: "m1"}
	var src []corev1.Secret
	src = append(src, secretObjs("shared", "db", `[{"application":"remote-1"}]`, "2", "1", "2")...)
	src = append(src, secretObjs("scoped", "db", `[{"application":"remote-1","relation":"database:5"}]`, "1", "1")...)
	src = append(src, secretObjs("other-relation", "db", `[{"application":"remote-1","relation":"9"}]`, "1", "1")...)
	src = append(src, secretObjs("unit", "db", `[{"application":"remote-1","unit":"remote-1/0"}]`, "1", "1")...)
	src = append(src, secretObjs("elsewhere", "db", `[{"application":"remote-2"}]`, "1", "1")...)
	src = append(src, secretObjs("not-granted", "db", ``, "1", "1")...)
	src = append(src, secretObjs("foreign", "another-app", `[{"application":"remote-1"}]`, "1", "1")...)
	src = append(src, secretObjs("garbled", "db", `not json`, "1", "1")...)
	mirrored := secretObjs("already", "db", `[{"application":"remote-1"}]`, "1", "1")
	for i := range mirrored {
		mirrored[i].Labels[v1alpha1.SecretMirrorLabel] = "m0"
	}
	src = append(src, mirrored...)

	got := MirrorSecrets(src, p)
	var names []string
	for _, s := range got {
		names = append(names, s.Name)
		if s.Namespace != "web" || s.Labels[v1alpha1.AppLabel] != "pg" || s.Labels[v1alpha1.SecretMirrorLabel] != "m1" || s.Type != corev1.SecretTypeOpaque {
			t.Errorf("%s: %+v", s.Name, s.ObjectMeta)
		}
	}
	want := []string{"jk-secret-scoped", "jk-secret-scoped-1", "jk-secret-shared", "jk-secret-shared-1", "jk-secret-shared-2", "jk-secret-unit", "jk-secret-unit-1"}
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("%v", names)
	}
	var meta, rev corev1.Secret
	for _, s := range got {
		switch s.Name {
		case "jk-secret-shared":
			meta = s
		case "jk-secret-shared-2":
			rev = s
		}
	}
	// The copy is granted to the consumer's own application and owned by the alias, whatever the original said.
	if meta.Annotations[v1alpha1.SecretGrantsAnnotation] != `[{"application":"client"}]` || meta.Annotations[v1alpha1.SecretOwnerAnnotation] != "application:pg" ||
		meta.Annotations[v1alpha1.SecretLatestAnnotation] != "2" || meta.Annotations["jk.luci1900.github.io/secret-label"] != "mine" {
		t.Fatalf("%v", meta.Annotations)
	}
	if string(rev.Data["token"]) != "t2" || rev.Labels[v1alpha1.SecretRevisionLabel] != "2" {
		t.Fatalf("%+v", rev)
	}
	// The data is a copy.
	rev.Data["token"][0] = 'X'
	for _, s := range src {
		if s.Name == "jk-secret-shared-2" && string(s.Data["token"]) != "t2" {
			t.Fatal("data aliased")
		}
	}
	if got := MirroredRevisions(got); !reflect.DeepEqual(got, map[string]int{"shared": 2, "scoped": 1, "unit": 1}) {
		t.Fatalf("%v", got)
	}
	if MirroredRevisions(nil) != nil {
		t.Fatal("empty")
	}
	if MirrorID("a", "b") == MirrorID("a", "c") || len(MirrorID("a", "b")) != 16 {
		t.Fatal(MirrorID("a", "b"))
	}
}
