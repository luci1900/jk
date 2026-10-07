package agent

import (
	"context"
	"crypto/sha256"
	"encoding/base32"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/internal/hooktools"
)

// Annotations on the metadata Secret of a juju secret (docs/design.md, Secrets). The grants annotation and the labels are shared
// with the operator (api/v1alpha1/names.go).
const (
	AnnSecretOwner       = v1alpha1.SecretOwnerAnnotation // application:<app> or unit:<app>/<n>
	AnnSecretLabel       = v1alpha1.Group + "/secret-label"
	AnnSecretDescription = v1alpha1.Group + "/secret-description"
	AnnSecretRotate      = v1alpha1.Group + "/secret-rotate"
	AnnSecretExpire      = v1alpha1.Group + "/secret-expire" // RFC 3339, of the latest revision
	AnnSecretLatest      = v1alpha1.SecretLatestAnnotation
	AnnSecretRevisions   = v1alpha1.Group + "/secret-revisions" // JSON list of the revisions that exist
	// AnnExecID records the hook execution that wrote an object, which makes a rerun after a crash idempotent.
	AnnExecID = v1alpha1.Group + "/exec-id"
)

var xidEncoding = base32.NewEncoding("0123456789abcdefghijklmnopqrstuv").WithPadding(base32.NoPadding)

// DeriveXID returns the secret id for the n-th secret a hook execution creates: 12 bytes of a hash of the execution id,
// as 20 base32hex characters (the shape of juju's secret ids). A rerun of the same execution derives the same ids.
func DeriveXID(execID string, n int) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s/secret/%d", execID, n)))
	return xidEncoding.EncodeToString(sum[:12])
}

// SecretMeta is the content of a metadata Secret.
type SecretMeta struct {
	XID         string
	OwnerKind   string // hooktools.OwnerApplication or OwnerUnit
	OwnerID     string // application name or unit name
	Label       string
	Description string
	Rotate      string
	Expire      *time.Time
	Latest      int
	Revisions   []int
	Grants      []v1alpha1.SecretGrant
}

func (m SecretMeta) ownerAnnotation() string { return m.OwnerKind + ":" + m.OwnerID }

// metaFromSecret parses a metadata Secret.
func metaFromSecret(s *corev1.Secret) SecretMeta {
	a := s.Annotations
	m := SecretMeta{XID: s.Labels[v1alpha1.SecretLabel], Label: a[AnnSecretLabel], Description: a[AnnSecretDescription], Rotate: a[AnnSecretRotate]}
	m.OwnerKind, m.OwnerID, _ = strings.Cut(a[AnnSecretOwner], ":")
	if t, err := time.Parse(time.RFC3339, a[AnnSecretExpire]); err == nil {
		m.Expire = &t
	}
	m.Latest, _ = strconv.Atoi(a[AnnSecretLatest])
	_ = json.Unmarshal([]byte(a[AnnSecretRevisions]), &m.Revisions)
	_ = json.Unmarshal([]byte(a[v1alpha1.SecretGrantsAnnotation]), &m.Grants)
	return m
}

func (m SecretMeta) annotations(execID string) map[string]string {
	a := map[string]string{AnnSecretOwner: m.ownerAnnotation(), AnnSecretLatest: strconv.Itoa(m.Latest)}
	set := func(k, v string) {
		if v != "" {
			a[k] = v
		}
	}
	set(AnnSecretLabel, m.Label)
	set(AnnSecretDescription, m.Description)
	set(AnnSecretRotate, m.Rotate)
	if m.Expire != nil {
		a[AnnSecretExpire] = m.Expire.UTC().Format(time.RFC3339)
	}
	revs, _ := json.Marshal(m.Revisions)
	a[AnnSecretRevisions] = string(revs)
	if len(m.Grants) > 0 {
		g, _ := json.Marshal(m.Grants)
		a[v1alpha1.SecretGrantsAnnotation] = string(g)
	}
	if execID != "" {
		a[AnnExecID] = execID
	}
	return a
}

// secretObjects builds the Secrets of a secret: labels and owner reference are common.
type secretWriter struct {
	kube  Kube
	app   string
	owner metav1.OwnerReference
}

func (w secretWriter) labels(xid string, rev int) map[string]string {
	l := map[string]string{v1alpha1.AppLabel: w.app, v1alpha1.SecretLabel: xid}
	if rev > 0 {
		l[v1alpha1.SecretRevisionLabel] = strconv.Itoa(rev)
	}
	return l
}

// createRevision creates the immutable Secret of one revision. It succeeds when a Secret written by the same execution
// exists already (a rerun after a crash).
func (w secretWriter) createRevision(ctx context.Context, xid string, rev int, content map[string][]byte, execID string) error {
	s := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: v1alpha1.SecretRevisionName(xid, rev), Labels: w.labels(xid, rev),
			Annotations: map[string]string{AnnExecID: execID}, OwnerReferences: []metav1.OwnerReference{w.owner}},
		Immutable: ptr(true),
		Type:      corev1.SecretTypeOpaque,
		Data:      content,
	}
	err := w.kube.CreateSecret(ctx, s)
	if apierrors.IsAlreadyExists(err) {
		cur, gerr := w.kube.GetSecret(ctx, s.Name)
		if gerr != nil {
			return gerr
		}
		if cur.Annotations[AnnExecID] == execID && execID != "" {
			return nil
		}
		return fmt.Errorf("secret revision %s already exists", s.Name)
	}
	return err
}

// writeMeta creates or updates the metadata Secret to m (a full replacement of the metadata, idempotent). create makes it
// fail when the Secret exists from another execution.
func (w secretWriter) writeMeta(ctx context.Context, m SecretMeta, execID string, create bool) error {
	name := v1alpha1.SecretMetadataName(m.XID)
	if create {
		s := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: w.labels(m.XID, 0), Annotations: m.annotations(execID),
			OwnerReferences: []metav1.OwnerReference{w.owner}}, Type: corev1.SecretTypeOpaque}
		err := w.kube.CreateSecret(ctx, s)
		if apierrors.IsAlreadyExists(err) {
			cur, gerr := w.kube.GetSecret(ctx, name)
			if gerr != nil {
				return gerr
			}
			if cur.Annotations[AnnExecID] == execID {
				return nil
			}
			return fmt.Errorf("secret %s already exists", name)
		}
		return err
	}
	for i := 0; ; i++ {
		cur, err := w.kube.GetSecret(ctx, name)
		if err != nil {
			return err
		}
		cur.Annotations = m.annotations(execID)
		err = w.kube.UpdateSecret(ctx, cur)
		if apierrors.IsConflict(err) && i < 5 {
			continue
		}
		return err
	}
}

func sortedInts(in []int) []int {
	out := append([]int(nil), in...)
	sort.Ints(out)
	return out
}

func containsInt(s []int, v int) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

func removeInts(s []int, drop ...int) []int {
	var out []int
	for _, x := range s {
		if !containsInt(drop, x) {
			out = append(out, x)
		}
	}
	return out
}

// grantTarget formats the grantee as juju's secret-info-get shows it.
func grantAccess(g v1alpha1.SecretGrant) hooktools.SecretAccess {
	target := "application-" + g.Application
	if g.Unit != "" {
		target = "unit-" + strings.ReplaceAll(g.Unit, "/", "-")
	}
	scope := "relation-" + g.Relation
	if g.Relation == "" {
		scope = "application-" + g.Application
	}
	return hooktools.SecretAccess{Target: target, Scope: scope, Role: "view"}
}
