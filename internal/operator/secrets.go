package operator

import (
	"encoding/json"
	"sort"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/luci1900/jk/api/v1alpha1"
)

// SecretsRoleName is the Role (and RoleBinding) that gives an app's ServiceAccount access to secrets by name.
func SecretsRoleName(app string) string { return app + "-jk-secrets" }

// CacheOptions limits the manager's Secret cache to the secrets jk manages, so the operator does not hold
// every Secret of the cluster in memory.
func CacheOptions() cache.Options {
	return cache.Options{ByObject: map[client.Object]cache.ByObject{
		&corev1.Secret{}: {Label: secretSelector()},
	}}
}

func secretSelector() labels.Selector {
	sel, err := labels.Parse(v1alpha1.SecretLabel) // "label exists"
	if err != nil {
		panic(err)
	}
	return sel
}

// SecretNames computes, for one application, the names of the Secrets it owns and of the ones granted to it,
// from the jk-managed Secrets of the namespace (those labelled with v1alpha1.SecretLabel). A secret's
// metadata Secret (jk-secret-<xid>) carries the grants; its revision Secrets (jk-secret-<xid>-<rev>) share the
// SecretLabel. A malformed grants annotation grants nothing. A grant scoped to a relation (juju: `secret-grant
// --relation`) ends with that relation: liveRelation says whether a relation id still exists (nil: all do).
func SecretNames(app string, secrets []corev1.Secret, liveRelation func(id int64) bool) (owned, granted []string) {
	grantees := map[string]map[string]bool{} // xid -> applications
	for i := range secrets {
		s := &secrets[i]
		xid := s.Labels[v1alpha1.SecretLabel]
		if xid != "" && s.Name == v1alpha1.SecretMetadataName(xid) {
			grantees[xid] = grantedApps(s, liveRelation)
		}
	}
	for i := range secrets {
		s := &secrets[i]
		xid := s.Labels[v1alpha1.SecretLabel]
		switch {
		case xid == "":
		case s.Labels[v1alpha1.AppLabel] == app:
			owned = append(owned, s.Name)
		case grantees[xid][app]:
			granted = append(granted, s.Name)
		}
	}
	sort.Strings(owned)
	sort.Strings(granted)
	return owned, granted
}

// GrantRelationID reads the relation id out of a grant's Relation: "7" or a relation key such as "database:7".
func GrantRelationID(rel string) (int64, bool) {
	if i := strings.LastIndex(rel, ":"); i >= 0 {
		rel = rel[i+1:]
	}
	id, err := strconv.ParseInt(rel, 10, 64)
	return id, err == nil && id > 0
}

func grantedApps(s *corev1.Secret, liveRelation func(id int64) bool) map[string]bool {
	var grants []v1alpha1.SecretGrant
	if err := json.Unmarshal([]byte(s.Annotations[v1alpha1.SecretGrantsAnnotation]), &grants); err != nil {
		return nil
	}
	out := map[string]bool{}
	for _, g := range grants {
		if g.Application == "" {
			continue
		}
		if id, ok := GrantRelationID(g.Relation); ok && liveRelation != nil && !liveRelation(id) {
			continue
		}
		out[g.Application] = true
	}
	return out
}

// SecretRules are the rules of the app's secrets Role: full access to the named Secrets it owns, read access to
// the ones granted to it. RBAC cannot select by label, so names are listed (list and watch work with a
// metadata.name field selector).
func SecretRules(owned, granted []string) []rbacv1.PolicyRule {
	var rules []rbacv1.PolicyRule
	if len(owned) > 0 {
		rules = append(rules, rbacv1.PolicyRule{APIGroups: []string{""}, Resources: []string{"secrets"}, ResourceNames: owned,
			Verbs: []string{"get", "list", "watch", "update", "patch", "delete"}})
	}
	if len(granted) > 0 {
		rules = append(rules, rbacv1.PolicyRule{APIGroups: []string{""}, Resources: []string{"secrets"}, ResourceNames: granted,
			Verbs: []string{"get", "list", "watch"}})
	}
	return rules
}

// secretRequests maps a Secret event to the applications whose secrets Role it can change: the owner and the
// granted applications.
func secretRequests(o client.Object) []reconcile.Request {
	var reqs []reconcile.Request
	add := func(app string) {
		if app != "" {
			reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: o.GetNamespace(), Name: app}})
		}
	}
	add(o.GetLabels()[v1alpha1.AppLabel])
	if s, ok := o.(*corev1.Secret); ok {
		for app := range grantedApps(s, nil) {
			add(app)
		}
	}
	return reqs
}
