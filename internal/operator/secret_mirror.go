package operator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"sort"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/luci1900/jk/api/v1alpha1"
)

// MirrorParams say which secrets a relation across namespaces shares, and how the copies look in the other namespace.
type MirrorParams struct {
	// SrcApp owns the secrets; Grantee is how SrcApp's namespace knows the application that receives them (the
	// alias of the destination's application), and RelationID the relation's id in the source namespace.
	SrcApp     string
	Grantee    string
	RelationID int64
	// DstNS and DstApp are the receiving namespace and application; OwnerAlias is the name the owner has there.
	DstNS      string
	DstApp     string
	OwnerAlias string
	// MirrorID identifies the relation on the copies.
	MirrorID string
}

// MirrorID is the value of the SecretMirrorLabel for a relation (label values are short).
func MirrorID(namespace, relation string) string {
	sum := sha256.Sum256([]byte(namespace + "/" + relation))
	return hex.EncodeToString(sum[:8])
}

// MirrorSecrets returns the copies of the secrets of src that SrcApp owns and has granted to Grantee (for the whole
// application, or for this relation) as they are in the destination namespace: owned by the alias, granted to DstApp,
// with every revision. The destination's agents read them like any secret granted to their application, and the
// operator's Roles by name give DstApp access.
func MirrorSecrets(src []corev1.Secret, p MirrorParams) []corev1.Secret {
	shared := map[string]bool{}
	for i := range src {
		s := &src[i]
		xid := s.Labels[v1alpha1.SecretLabel]
		if xid == "" || s.Name != v1alpha1.SecretMetadataName(xid) || s.Labels[v1alpha1.AppLabel] != p.SrcApp || s.Labels[v1alpha1.SecretMirrorLabel] != "" {
			continue
		}
		var grants []v1alpha1.SecretGrant
		if json.Unmarshal([]byte(s.Annotations[v1alpha1.SecretGrantsAnnotation]), &grants) != nil {
			continue
		}
		for _, g := range grants {
			if g.Application != p.Grantee {
				continue
			}
			if id, ok := GrantRelationID(g.Relation); g.Relation != "" && (!ok || id != p.RelationID) {
				continue
			}
			shared[xid] = true
		}
	}
	var out []corev1.Secret
	for i := range src {
		s := &src[i]
		xid := s.Labels[v1alpha1.SecretLabel]
		if !shared[xid] {
			continue
		}
		cp := corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: p.DstNS, Name: s.Name,
				Labels:      map[string]string{v1alpha1.SecretLabel: xid, v1alpha1.AppLabel: p.OwnerAlias, v1alpha1.SecretMirrorLabel: p.MirrorID},
				Annotations: map[string]string{},
			},
			Type: corev1.SecretTypeOpaque,
		}
		if rev, ok := s.Labels[v1alpha1.SecretRevisionLabel]; ok {
			cp.Labels[v1alpha1.SecretRevisionLabel] = rev
			cp.Data = copyBytes(s.Data)
		} else {
			for k, v := range s.Annotations {
				cp.Annotations[k] = v
			}
			grants, _ := json.Marshal([]v1alpha1.SecretGrant{{Application: p.DstApp}})
			cp.Annotations[v1alpha1.SecretGrantsAnnotation] = string(grants)
			cp.Annotations[v1alpha1.SecretOwnerAnnotation] = "application:" + p.OwnerAlias
		}
		out = append(out, cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func copyBytes(m map[string][]byte) map[string][]byte {
	if m == nil {
		return nil
	}
	out := make(map[string][]byte, len(m))
	for k, v := range m {
		out[k] = append([]byte(nil), v...)
	}
	return out
}

// MirroredRevisions maps each mirrored secret to its latest revision.
func MirroredRevisions(mirrors []corev1.Secret) map[string]int {
	out := map[string]int{}
	for i := range mirrors {
		s := &mirrors[i]
		xid := s.Labels[v1alpha1.SecretLabel]
		if s.Name != v1alpha1.SecretMetadataName(xid) {
			continue
		}
		n, _ := strconv.Atoi(s.Annotations[v1alpha1.SecretLatestAnnotation])
		out[xid] = n
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// syncMirrors makes the copies in dst (those labelled with mirrorID) equal to want: missing ones are created, changed
// ones updated, and the ones no longer shared deleted. A Secret of that name that is not a copy for this relation is
// left alone.
func (r *RelationReconciler) syncMirrors(ctx context.Context, dst, mirrorID string, want []corev1.Secret) error {
	var have corev1.SecretList
	if err := r.List(ctx, &have, client.InNamespace(dst), client.MatchingLabels{v1alpha1.SecretMirrorLabel: mirrorID}); err != nil {
		return err
	}
	keep := map[string]bool{}
	for i := range want {
		w := &want[i]
		keep[w.Name] = true
		var cur corev1.Secret
		err := r.Get(ctx, client.ObjectKeyFromObject(w), &cur)
		switch {
		case apierrors.IsNotFound(err):
			if err := r.Create(ctx, w); err != nil && !apierrors.IsAlreadyExists(err) {
				return err
			}
		case err != nil:
			return err
		case cur.Labels[v1alpha1.SecretMirrorLabel] != mirrorID:
			// Another relation's copy (or a secret of the namespace's own): not ours to change.
		case !reflect.DeepEqual(cur.Data, w.Data) || !reflect.DeepEqual(cur.Annotations, w.Annotations) || !reflect.DeepEqual(cur.Labels, w.Labels):
			cur.Data, cur.Annotations, cur.Labels = w.Data, w.Annotations, w.Labels
			if err := r.Update(ctx, &cur); err != nil {
				return err
			}
		}
	}
	for i := range have.Items {
		if s := &have.Items[i]; !keep[s.Name] {
			if err := r.Delete(ctx, s); client.IgnoreNotFound(err) != nil {
				return err
			}
		}
	}
	return nil
}

// secretsIn lists the jk-managed Secrets of a namespace.
func (r *RelationReconciler) secretsIn(ctx context.Context, ns string) ([]corev1.Secret, error) {
	var l corev1.SecretList
	if err := r.List(ctx, &l, client.InNamespace(ns)); err != nil {
		return nil, err
	}
	return l.Items, nil
}
