// SPDX-License-Identifier: AGPL-3.0-only
//
// Derived from juju (https://github.com/juju/juju), AGPL-3.0: internal/worker/uniter/runner/context/secrets.go and
// context.go (secretsChangeRecorder).

package agent

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/internal/agent/resolver"
	"github.com/luci1900/jk/internal/hooktools"
)

// pendingSecret is what a hook did to one secret; it is written when the hook succeeds.
type pendingSecret struct {
	xid       string
	create    bool
	ownerKind string

	label, description, rotate *string
	expire                     *time.Time
	content                    map[string][]byte

	removeAll  bool
	removeRevs []int
	grants     []v1alpha1.SecretGrant
	revokes    []v1alpha1.SecretGrant
}

// secretRecorder collects the secret changes of a hook (juju's secretsChangeRecorder).
type secretRecorder struct {
	order   []string
	items   map[string]*pendingSecret
	created int
	// tracked are the consumer records changed in this hook (secret-get with --refresh or a label).
	tracked map[string]v1alpha1.TrackedSecret
}

func newSecretRecorder() secretRecorder {
	return secretRecorder{items: map[string]*pendingSecret{}, tracked: map[string]v1alpha1.TrackedSecret{}}
}

func (r *secretRecorder) get(xid string) *pendingSecret {
	if p, ok := r.items[xid]; ok {
		return p
	}
	p := &pendingSecret{xid: xid}
	r.items[xid] = p
	r.order = append(r.order, xid)
	return p
}

func (r *secretRecorder) drop(xid string) {
	delete(r.items, xid)
	for i, x := range r.order {
		if x == xid {
			r.order = append(r.order[:i], r.order[i+1:]...)
			break
		}
	}
}

// secretIndexChanges are the changes to the owner indexes (UnitData and AppData) that a hook's secret writes imply.
type secretIndexChanges struct {
	unit map[string]*v1alpha1.OwnedSecret // nil value: removed
	app  map[string]*v1alpha1.OwnedSecret
}

func (c *secretIndexChanges) appChanged() bool { return c != nil && len(c.app) > 0 }

func applyIndex(dst *map[string]v1alpha1.OwnedSecret, ch map[string]*v1alpha1.OwnedSecret) {
	for xid, e := range ch {
		if e == nil {
			delete(*dst, xid)
			continue
		}
		if *dst == nil {
			*dst = map[string]v1alpha1.OwnedSecret{}
		}
		(*dst)[xid] = *e
	}
	if len(*dst) == 0 {
		*dst = nil
	}
}

func (c *secretIndexChanges) applyApp(sp *v1alpha1.AppDataSpec) { applyIndex(&sp.OwnedSecrets, c.app) }

// secretRetry bounds how long a read waits for the operator to grant access to a new secret.
const secretReadAttempts = 10

func (h *hookContext) secretOwnerMatches(e ownedEntry) bool {
	return (e.OwnerKind == hooktools.OwnerApplication && e.OwnerID == h.a.cfg.App) || (e.OwnerKind == hooktools.OwnerUnit && e.OwnerID == h.a.cfg.Unit)
}

// canManage is true for the owner: the unit of a unit-owned secret, the leader for an application-owned one.
func (h *hookContext) canManage(e ownedEntry) (bool, error) {
	switch {
	case e.OwnerKind == hooktools.OwnerUnit && e.OwnerID == h.a.cfg.Unit:
		return true, nil
	case e.OwnerKind == hooktools.OwnerApplication && e.OwnerID == h.a.cfg.App:
		if !h.a.lead.IsLeader() {
			return false, hooktools.ErrNotLeader
		}
		return true, nil
	}
	return false, nil
}

func xidOf(uri string) string { return strings.TrimPrefix(uri, "secret:") }

// effectiveOwned is the owned index with this hook's pending changes: what secret-ids and secret-info-get show.
func (h *hookContext) effectiveOwned() map[string]ownedEntry {
	out := h.a.ownedSecrets(h.w)
	for _, xid := range h.sec.order {
		p := h.sec.items[xid]
		if p.removeAll {
			delete(out, xid)
			continue
		}
		e := out[xid]
		if p.create {
			e = ownedEntry{OwnedSecret: v1alpha1.OwnedSecret{Revision: 1}, OwnerKind: p.ownerKind}
			if p.ownerKind == hooktools.OwnerApplication {
				e.OwnerID = h.a.cfg.App
			} else {
				e.OwnerID = h.a.cfg.Unit
			}
		}
		if p.label != nil {
			e.Label = *p.label
		}
		out[xid] = e
	}
	return out
}

func (h *hookContext) CreateSecret(c hooktools.SecretCreate) (string, error) {
	if c.Owner == hooktools.OwnerApplication && !h.a.lead.IsLeader() {
		return "", hooktools.ErrNotLeader
	}
	if len(c.Content) == 0 {
		return "", fmt.Errorf("empty secret content not valid")
	}
	if c.Label != "" {
		for _, e := range h.effectiveOwned() {
			if e.Label == c.Label && h.secretOwnerMatches(e) {
				return "", fmt.Errorf("secret with label %q already exists", c.Label)
			}
		}
	}
	xid := DeriveXID(h.execID, h.sec.created)
	h.sec.created++
	p := h.sec.get(xid)
	p.create, p.ownerKind, p.content = true, c.Owner, c.Content
	if c.Label != "" {
		p.label = &c.Label
	}
	if c.Description != "" {
		p.description = &c.Description
	}
	if c.RotatePolicy != "" {
		p.rotate = &c.RotatePolicy
	}
	p.expire = c.Expire
	return hooktools.SecretURI(xid), nil
}

// ownedForWrite finds an owned secret the unit may change.
func (h *hookContext) ownedForWrite(uri string) (string, ownedEntry, error) {
	xid := xidOf(uri)
	if p, ok := h.sec.items[xid]; ok && p.create {
		return xid, ownedEntry{OwnerKind: p.ownerKind}, nil
	}
	e, ok := h.effectiveOwned()[xid]
	if !ok {
		// App-owned secrets are only listed to the leader; say why.
		if ae, found := h.a.findOwned(h.w, xid); found && h.ownedByThisApp(ae) {
			if _, err := h.canManage(ae); err != nil {
				return "", ownedEntry{}, err
			}
			return "", ownedEntry{}, hooktools.ErrPermission
		}
		return "", ownedEntry{}, hooktools.NotFoundf("secret %q", uri)
	}
	if ok, err := h.canManage(e); err != nil {
		return "", ownedEntry{}, err
	} else if !ok {
		return "", ownedEntry{}, hooktools.ErrPermission
	}
	return xid, e, nil
}

// ownedByThisApp tells the application's own secrets from those other applications shared with it, which a
// consumer can read but, as in juju, does not know as secrets it could manage.
func (h *hookContext) ownedByThisApp(e ownedEntry) bool {
	if e.OwnerKind == hooktools.OwnerApplication {
		return e.OwnerID == h.a.cfg.App
	}
	return resolver.UnitApp(e.OwnerID) == h.a.cfg.App
}

func (h *hookContext) UpdateSecret(uri string, upd hooktools.SecretUpdate) error {
	xid, e, err := h.ownedForWrite(uri)
	if err != nil {
		return err
	}
	p := h.sec.get(xid)
	if upd.Label != nil {
		p.label = upd.Label
	}
	if upd.Description != nil {
		p.description = upd.Description
	}
	if upd.RotatePolicy != nil {
		p.rotate = upd.RotatePolicy
	}
	if upd.Expire != nil {
		p.expire = upd.Expire
	}
	if len(upd.Content) > 0 {
		// An unchanged value does not make a revision.
		if !p.create {
			if cur, err := h.revisionContent(xid, e.Revision); err == nil && equalContent(cur, upd.Content) {
				return nil
			}
		}
		p.content = upd.Content
	}
	return nil
}

func equalContent(a, b map[string][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || !bytes.Equal(v, w) {
			return false
		}
	}
	return true
}

func (h *hookContext) RemoveSecret(uri string, revision *int) error {
	xid, _, err := h.ownedForWrite(uri)
	if err != nil {
		return err
	}
	if p, ok := h.sec.items[xid]; ok && p.create {
		if revision == nil || *revision == 1 {
			h.sec.drop(xid)
		}
		return nil
	}
	p := h.sec.get(xid)
	p.content, p.grants, p.revokes = nil, nil, nil
	if revision == nil {
		p.removeAll = true
	} else if !p.removeAll {
		p.removeRevs = append(p.removeRevs, *revision)
	}
	return nil
}

// revisionContent reads the content of one revision, waiting a little for access that the operator grants by name.
func (h *hookContext) revisionContent(xid string, rev int) (map[string][]byte, error) {
	var lastErr error
	for i := 0; i < secretReadAttempts; i++ {
		s, err := h.a.kube.GetSecret(h.ctx, v1alpha1.SecretRevisionName(xid, rev))
		if err == nil {
			return s.Data, nil
		}
		lastErr = err
		if apierrors.IsNotFound(err) {
			return nil, hooktools.NotFoundf("secret revision %d", rev)
		}
		if !apierrors.IsForbidden(err) {
			break
		}
		time.Sleep(h.a.cfg.SecretRetryDelay)
	}
	if apierrors.IsForbidden(lastErr) {
		return nil, hooktools.ErrPermission
	}
	return nil, lastErr
}

func (h *hookContext) metadata(xid string) (SecretMeta, error) {
	s, err := h.a.kube.GetSecret(h.ctx, v1alpha1.SecretMetadataName(xid))
	if err != nil {
		if apierrors.IsNotFound(err) {
			return SecretMeta{}, hooktools.NotFoundf("secret %q", hooktools.SecretURI(xid))
		}
		if apierrors.IsForbidden(err) {
			return SecretMeta{}, hooktools.ErrPermission
		}
		return SecretMeta{}, err
	}
	return metaFromSecret(s), nil
}

// mayRead says whether this unit may read a secret it does not own: it needs a grant for its application or itself,
// and a grant scoped to a relation only counts while the unit is in that relation.
func (h *hookContext) mayRead(m SecretMeta) bool {
	return h.mayReadGrants(m.OwnerKind, m.OwnerID, m.Grants)
}

func (h *hookContext) mayReadGrants(ownerKind, ownerID string, grants []v1alpha1.SecretGrant) bool {
	switch {
	case ownerKind == hooktools.OwnerApplication && ownerID == h.a.cfg.App:
		return true
	case ownerKind == hooktools.OwnerUnit && ownerID == h.a.cfg.Unit:
		return true
	}
	for _, g := range grants {
		if g.Application != h.a.cfg.App || (g.Unit != "" && g.Unit != h.a.cfg.Unit) {
			continue
		}
		if g.Relation != "" {
			id, err := strconv.Atoi(g.Relation)
			if err != nil || !h.local.Relations[id].InScope {
				continue
			}
		}
		return true
	}
	return false
}

func (h *hookContext) currentTracked(uri string) (v1alpha1.TrackedSecret, bool) {
	if t, ok := h.sec.tracked[uri]; ok {
		return t, true
	}
	t, ok := h.a.store.Spec().TrackedSecrets[uri]
	return t, ok
}

func (h *hookContext) GetSecret(uriIn, label string, refresh, peek bool) (map[string][]byte, error) {
	var xid string
	if uriIn != "" {
		xid = xidOf(uriIn)
	} else {
		// A label is the owner's label (for application-owned secrets any unit of the application may use it),
		// or the label this unit gave when it consumed the secret.
		for x, p := range h.sec.items {
			if p.label != nil && *p.label == label && p.create {
				return p.content, nil
			}
			_ = x
		}
		for x, e := range h.effectiveOwned() {
			if e.Label == label && label != "" {
				xid = x
				break
			}
		}
		if xid == "" {
			if ae := h.appOwnedByLabel(label); ae != "" {
				xid = ae
			}
		}
		if xid == "" {
			for u, t := range h.sec.tracked {
				if t.Label == label {
					xid = xidOf(u)
				}
			}
		}
		if xid == "" {
			for u, t := range h.a.store.Spec().TrackedSecrets {
				if t.Label == label {
					xid = xidOf(u)
				}
			}
		}
		if xid == "" {
			return nil, hooktools.NotFoundf("secret with label %q", label)
		}
	}
	uri := hooktools.SecretURI(xid)
	if p, ok := h.sec.items[xid]; ok && p.create {
		return p.content, nil
	}

	e, indexed := h.a.findOwned(h.w, xid)
	switch {
	case indexed && (h.secretOwnerMatches(e) || h.mayReadGrants(e.OwnerKind, e.OwnerID, e.Grants)):
		// The owner, or a grantee according to the owner's index (which is written before the secret is shared).
	default:
		// Not indexed (yet), or not granted according to the index, which may lag: the metadata Secret decides.
		m, err := h.metadata(xid)
		if err != nil {
			return nil, err
		}
		if !h.mayRead(m) {
			return nil, hooktools.ErrPermission
		}
		if !indexed {
			e = ownedEntry{OwnedSecret: v1alpha1.OwnedSecret{Label: m.Label, Revision: m.Latest, Revisions: m.Revisions}, OwnerKind: m.OwnerKind, OwnerID: m.OwnerID}
		}
	}

	// The owner's pending update is visible to the owner's refresh and peek.
	if p, ok := h.sec.items[xid]; ok && len(p.content) > 0 && (refresh || peek) {
		if refresh {
			h.sec.tracked[uri] = v1alpha1.TrackedSecret{Revision: e.Revision + 1, Label: h.sec.tracked[uri].Label}
		}
		return p.content, nil
	}

	tr, tracked := h.currentTracked(uri)
	doRefresh := refresh || !tracked
	rev := tr.Revision
	if doRefresh || peek {
		rev = e.Revision
	}
	changed := !tracked
	if doRefresh && tr.Revision != e.Revision {
		tr.Revision, changed = e.Revision, true
	}
	// The owner's label identifies the secret to the owner and, for application-owned secrets, the application's units;
	// consumers get to choose their own.
	if label != "" && uriIn != "" && !h.secretOwnerMatches(e) && tr.Label != label {
		tr.Label, changed = label, true
	}
	if changed {
		h.sec.tracked[uri] = tr
	}
	return h.revisionContent(xid, rev)
}

func (h *hookContext) appOwnedByLabel(label string) string {
	if label == "" {
		return ""
	}
	if ad := h.w.appData[h.a.cfg.App]; ad != nil {
		for xid, e := range ad.Spec.OwnedSecrets {
			if e.Label == label {
				return xid
			}
		}
	}
	return ""
}

func (h *hookContext) SecretMetadata() (map[string]hooktools.SecretMetadata, error) {
	out := map[string]hooktools.SecretMetadata{}
	for xid, e := range h.effectiveOwned() {
		md := hooktools.SecretMetadata{Label: e.Label, Owner: e.OwnerKind, LatestRevision: e.Revision}
		if e.ExpireTime != nil {
			t := e.ExpireTime.Time
			md.Expire = &t
		}
		p := h.sec.items[xid]
		if p == nil || !p.create {
			if m, err := h.metadata(xid); err == nil {
				md.Description, md.RotatePolicy = m.Description, m.Rotate
				if m.Expire != nil {
					md.Expire = m.Expire
				}
				for _, g := range m.Grants {
					if p == nil || !grantIn(p.revokes, g) {
						md.Access = append(md.Access, grantAccess(g))
					}
				}
			}
		}
		if p != nil {
			if p.description != nil {
				md.Description = *p.description
			}
			if p.rotate != nil {
				md.RotatePolicy = *p.rotate
			}
			if p.expire != nil {
				md.Expire = p.expire
			}
			for _, g := range p.grants {
				md.Access = append(md.Access, grantAccess(g))
			}
			if len(p.content) > 0 && !p.create {
				md.LatestRevision = e.Revision + 1
			}
		}
		out[xid] = md
	}
	return out, nil
}

func grantIn(list []v1alpha1.SecretGrant, g v1alpha1.SecretGrant) bool {
	for _, x := range list {
		if x.Application == g.Application && (x.Unit == "" || x.Unit == g.Unit) {
			return true
		}
	}
	return false
}

func (h *hookContext) GrantSecret(uri string, g hooktools.SecretGrant) error {
	xid, _, err := h.ownedForWrite(uri)
	if err != nil {
		return err
	}
	p := h.sec.get(xid)
	ng := v1alpha1.SecretGrant{Application: g.Application, Unit: g.Unit, Relation: g.Relation}
	existing := append([]v1alpha1.SecretGrant(nil), p.grants...)
	if !p.create {
		if m, err := h.metadata(xid); err == nil {
			existing = append(existing, m.Grants...)
		}
	}
	for _, x := range existing {
		if x.Application != ng.Application {
			continue
		}
		switch {
		case x.Unit == ng.Unit:
			return nil // already granted
		case x.Unit == "":
			return nil // the application has access, so its units do
		case ng.Unit == "":
			return fmt.Errorf("any unit level grants need to be revoked before granting access to the corresponding application")
		}
	}
	p.grants = append(p.grants, ng)
	return nil
}

func (h *hookContext) RevokeSecret(uri string, g hooktools.SecretGrant) error {
	xid, _, err := h.ownedForWrite(uri)
	if err != nil {
		return err
	}
	p := h.sec.get(xid)
	p.revokes = append(p.revokes, v1alpha1.SecretGrant{Application: g.Application, Unit: g.Unit, Relation: g.Relation})
	return nil
}

// revokeBrokenRelationGrants revokes the grants scoped to a relation that is being broken from the secrets this unit
// manages (juju drops them when the relation goes), so that the other application loses access with the relation.
func (h *hookContext) revokeBrokenRelationGrants() {
	if h.info.Kind != resolver.RelationBroken {
		return
	}
	rel := strconv.Itoa(h.info.RelationID)
	for xid, e := range h.a.ownedSecrets(h.w) {
		if p, ok := h.sec.items[xid]; ok && (p.create || p.removeAll) {
			continue
		}
		if ok, err := h.canManage(e); err != nil || !ok {
			continue
		}
		m, err := h.metadata(xid)
		if err != nil {
			continue
		}
		for _, g := range m.Grants {
			if g.Relation == rel {
				p := h.sec.get(xid)
				p.revokes = append(p.revokes, v1alpha1.SecretGrant{Application: g.Application, Unit: g.Unit, Relation: rel})
			}
		}
	}
}

// commitSecrets writes the secret changes of the hook to the Secrets and returns the index changes.
func (h *hookContext) commitSecrets(ctx context.Context) (*secretIndexChanges, error) {
	h.revokeBrokenRelationGrants()
	idx := &secretIndexChanges{unit: map[string]*v1alpha1.OwnedSecret{}, app: map[string]*v1alpha1.OwnedSecret{}}
	if len(h.sec.order) == 0 {
		return idx, nil
	}
	app, err := h.a.kube.Application(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting application for secrets: %w", err)
	}
	appOwner := ownerRef(app)
	var unitOwner *metav1.OwnerReference
	unitRef := func() (metav1.OwnerReference, error) {
		if unitOwner != nil {
			return *unitOwner, nil
		}
		ud, err := h.a.kube.UnitData(ctx)
		if err != nil || ud == nil {
			return metav1.OwnerReference{}, fmt.Errorf("getting UnitData for secrets: %v", err)
		}
		ref := metav1.OwnerReference{APIVersion: v1alpha1.GroupVersion.String(), Kind: "UnitData", Name: ud.Name, UID: ud.UID}
		unitOwner = &ref
		return ref, nil
	}
	for _, xid := range h.sec.order {
		p := h.sec.items[xid]
		kind := p.ownerKind
		var entry ownedEntry
		if !p.create {
			e, ok := h.a.ownedSecrets(h.w)[xid]
			if !ok {
				if h.a.lead.IsLeader() {
					e, ok = h.a.findOwned(h.w, xid)
				}
				if !ok {
					// Already removed by an earlier run of this execution.
					continue
				}
			}
			entry, kind = e, e.OwnerKind
		}
		w := secretWriter{kube: h.a.kube, app: h.a.cfg.App, owner: appOwner}
		if kind == hooktools.OwnerUnit {
			ref, err := unitRef()
			if err != nil {
				return nil, err
			}
			w.owner = ref
		}
		var res *v1alpha1.OwnedSecret
		var removed bool
		switch {
		case p.create:
			res, err = h.createSecret(ctx, w, p)
		case p.removeAll || len(p.removeRevs) > 0:
			res, removed, err = h.removeSecret(ctx, w, p, entry)
		default:
			res, err = h.updateSecret(ctx, w, p, entry)
		}
		if err != nil {
			return nil, fmt.Errorf("secret %s: %w", hooktools.SecretURI(xid), err)
		}
		target := idx.unit
		if kind == hooktools.OwnerApplication {
			target = idx.app
		}
		if removed {
			target[xid] = nil
		} else if res != nil {
			target[xid] = res
		}
	}
	return idx, nil
}

func metaFromPending(w secretWriter, p *pendingSecret, owner string) SecretMeta {
	m := SecretMeta{XID: p.xid, OwnerKind: p.ownerKind, OwnerID: owner, Latest: 1, Revisions: []int{1}, Expire: p.expire, Grants: p.grants}
	if p.label != nil {
		m.Label = *p.label
	}
	if p.description != nil {
		m.Description = *p.description
	}
	if p.rotate != nil {
		m.Rotate = *p.rotate
	}
	return m
}

func (h *hookContext) createSecret(ctx context.Context, w secretWriter, p *pendingSecret) (*v1alpha1.OwnedSecret, error) {
	owner := h.a.cfg.App
	if p.ownerKind == hooktools.OwnerUnit {
		owner = h.a.cfg.Unit
	}
	if err := w.createRevision(ctx, p.xid, 1, p.content, h.execID); err != nil {
		return nil, err
	}
	m := metaFromPending(w, p, owner)
	if err := w.writeMeta(ctx, m, h.execID, true); err != nil {
		return nil, err
	}
	return ownedFromMeta(m), nil
}

func ownedFromMeta(m SecretMeta) *v1alpha1.OwnedSecret {
	o := &v1alpha1.OwnedSecret{Label: m.Label, Revision: m.Latest, Revisions: sortedInts(m.Revisions)}
	if len(m.Grants) > 0 {
		o.Grants = append([]v1alpha1.SecretGrant(nil), m.Grants...)
	}
	if m.Expire != nil {
		t := metav1.NewTime(*m.Expire)
		o.ExpireTime = &t
	}
	return o
}

func (h *hookContext) updateSecret(ctx context.Context, w secretWriter, p *pendingSecret, e ownedEntry) (*v1alpha1.OwnedSecret, error) {
	s, err := h.a.kube.GetSecret(ctx, v1alpha1.SecretMetadataName(p.xid))
	if err != nil {
		return nil, err
	}
	m := metaFromSecret(s)
	uri := hooktools.SecretURI(p.xid)
	if len(p.content) > 0 {
		newRev := m.Latest + 1
		// A rerun of this execution finds its own revision already written.
		if cur, err := h.a.kube.GetSecret(ctx, v1alpha1.SecretRevisionName(p.xid, m.Latest)); err == nil && cur.Annotations[AnnExecID] == h.execID {
			newRev = m.Latest
		} else if err := w.createRevision(ctx, p.xid, newRev, p.content, h.execID); err != nil {
			return nil, err
		}
		m.Latest = newRev
		if !containsInt(m.Revisions, newRev) {
			m.Revisions = append(m.Revisions, newRev)
		}
		// The owner does not get secret-changed for its own revision: its record moves along.
		if t, ok := h.currentTracked(uri); ok {
			t.Revision = newRev
			h.sec.tracked[uri] = t
		}
	}
	if p.label != nil {
		m.Label = *p.label
	}
	if p.description != nil {
		m.Description = *p.description
	}
	if p.rotate != nil {
		m.Rotate = *p.rotate
	}
	if p.expire != nil {
		m.Expire = p.expire
	}
	for _, g := range p.revokes {
		var keep []v1alpha1.SecretGrant
		for _, x := range m.Grants {
			if x.Application == g.Application && (g.Unit == "" || x.Unit == g.Unit) && (g.Relation == "" || x.Relation == g.Relation) {
				continue
			}
			keep = append(keep, x)
		}
		m.Grants = keep
	}
	for _, g := range p.grants {
		if !hasGrant(m.Grants, g) {
			m.Grants = append(m.Grants, g)
		}
	}
	if err := w.writeMeta(ctx, m, h.execID, false); err != nil {
		return nil, err
	}
	return ownedFromMeta(m), nil
}

func hasGrant(list []v1alpha1.SecretGrant, g v1alpha1.SecretGrant) bool {
	for _, x := range list {
		if x == g || (x.Application == g.Application && x.Unit == "" && g.Unit != "") {
			return true
		}
	}
	return false
}

func (h *hookContext) removeSecret(ctx context.Context, w secretWriter, p *pendingSecret, e ownedEntry) (*v1alpha1.OwnedSecret, bool, error) {
	s, err := h.a.kube.GetSecret(ctx, v1alpha1.SecretMetadataName(p.xid))
	if apierrors.IsNotFound(err) {
		return nil, p.removeAll, nil
	}
	if err != nil {
		return nil, false, err
	}
	m := metaFromSecret(s)
	revs := p.removeRevs
	if p.removeAll {
		revs = m.Revisions
		if len(revs) == 0 {
			revs = e.Revisions
		}
	}
	for _, r := range revs {
		if err := h.a.kube.DeleteSecret(ctx, v1alpha1.SecretRevisionName(p.xid, r)); err != nil {
			return nil, false, err
		}
	}
	if p.removeAll {
		if err := h.a.kube.DeleteSecret(ctx, v1alpha1.SecretMetadataName(p.xid)); err != nil {
			return nil, false, err
		}
		return nil, true, nil
	}
	m.Revisions = removeInts(m.Revisions, revs...)
	if err := w.writeMeta(ctx, m, h.execID, false); err != nil {
		return nil, false, err
	}
	return ownedFromMeta(m), false, nil
}

// applyUnitSecrets applies the unit-owned secret index changes and the consumer records to the UnitData spec.
func (h *hookContext) applyUnitSecrets(sp *v1alpha1.UnitDataSpec, idx *secretIndexChanges) {
	if idx != nil {
		applyIndex(&sp.OwnedSecrets, idx.unit)
	}
	if len(h.sec.tracked) == 0 {
		return
	}
	uris := make([]string, 0, len(h.sec.tracked))
	for u := range h.sec.tracked {
		uris = append(uris, u)
	}
	sort.Strings(uris)
	if sp.TrackedSecrets == nil {
		sp.TrackedSecrets = map[string]v1alpha1.TrackedSecret{}
	}
	for _, u := range uris {
		sp.TrackedSecrets[u] = h.sec.tracked[u]
	}
}
