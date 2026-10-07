package agent

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/internal/agent/resolver"
)

// relationMeta is what the agent knows about a relation from its Relation object.
type relationMeta struct {
	Endpoint  string
	RemoteApp string
	Peer      bool
}

// unitNameOf turns the name of a UnitData (<app>-<n>) into the unit name (<app>/<n>).
func unitNameOf(dataName string) string {
	i := strings.LastIndexByte(dataName, '-')
	if i <= 0 {
		return dataName
	}
	return dataName[:i] + "/" + dataName[i+1:]
}

func appOfUnitData(ud *v1alpha1.UnitData) string {
	if a := ud.Labels[v1alpha1.AppLabel]; a != "" {
		return a
	}
	return resolver.UnitApp(unitNameOf(ud.Name))
}

// relationMetaOf reads a Relation object: the local endpoint, the application on the other side, and whether it is a peer
// relation. ok is false for relations of other applications, relations without an id yet (ids start at 1), and
// endpoints the charm does not have.
func (a *Agent) relationMetaOf(r *v1alpha1.Relation) (id int, m relationMeta, ok bool) {
	if r.Status.ID <= 0 {
		return 0, m, false
	}
	var local, remote *v1alpha1.EndpointRef
	for i := range r.Spec.Endpoints {
		e := &r.Spec.Endpoints[i]
		if e.Namespace == a.cfg.Namespace && e.Application == a.cfg.App && local == nil {
			local = e
		} else {
			remote = e
		}
	}
	if local == nil || !a.charm.HasEndpoint(local.Endpoint) {
		return 0, m, false
	}
	m = relationMeta{Endpoint: local.Endpoint, RemoteApp: a.cfg.App, Peer: remote == nil}
	if remote != nil {
		m.RemoteApp = remote.Application
		if remote.Namespace != a.cfg.Namespace {
			// An application of another namespace is known by its alias, so it cannot clash with a local one.
			m.RemoteApp = remoteAlias(r)
		}
	}
	return int(r.Status.ID), m, true
}

// remoteAlias is the name the application on the other side of a relation to another namespace has here: the
// alias, else the offer's name.
func remoteAlias(r *v1alpha1.Relation) string {
	if r.Spec.Alias != "" {
		return r.Spec.Alias
	}
	return r.Spec.Offer
}

// addRemote turns the RemoteData of relations to other namespaces into the units and application data of the remote
// application (known by its alias), so that the resolver and the hook tools read them like any other relation's.
func (a *Agent) addRemote(w *world, remote []v1alpha1.RemoteData) {
	ids := map[string]int64{}
	for i := range w.relations {
		r := &w.relations[i]
		if r.Status.ID > 0 {
			ids[r.Name] = r.Status.ID
		}
	}
	for i := range remote {
		rd := &remote[i]
		if id, ok := ids[rd.Name]; !ok || id != rd.Spec.RelationID || rd.Spec.Application == "" {
			continue // for a relation that is gone or has another id: stale
		}
		key := relKey(int(rd.Spec.RelationID))
		alias := rd.Spec.Application
		for unit, ru := range rd.Spec.Units {
			w.units = append(w.units, v1alpha1.UnitData{
				ObjectMeta: metav1.ObjectMeta{Name: strings.ReplaceAll(unit, "/", "-"), Labels: map[string]string{v1alpha1.AppLabel: alias}},
				Spec:       v1alpha1.UnitDataSpec{Relations: map[string]v1alpha1.UnitRelationState{key: {InScope: true, Data: ru.Data}}},
			})
		}
		w.appData[alias] = &v1alpha1.AppData{
			ObjectMeta: metav1.ObjectMeta{Name: alias},
			Spec:       v1alpha1.AppDataSpec{Relations: map[string]v1alpha1.RelationData{key: rd.Spec.AppData}},
		}
	}
}

// relationSnapshots is the remote state of the unit's relations: members are the other units of the remote application
// that are in scope, each with the version of its settings.
func (a *Agent) relationSnapshots(w *world) map[int]resolver.RelationSnapshot {
	out := map[int]resolver.RelationSnapshot{}
	units := map[string][]*v1alpha1.UnitData{}
	for i := range w.units {
		ud := &w.units[i]
		units[appOfUnitData(ud)] = append(units[appOfUnitData(ud)], ud)
	}
	for i := range w.relations {
		r := &w.relations[i]
		id, m, ok := a.relationMetaOf(r)
		if !ok {
			continue
		}
		if a.relMeta == nil {
			a.relMeta = map[int]relationMeta{}
		}
		a.relMeta[id] = m
		rs := resolver.RelationSnapshot{Peer: m.Peer, Endpoint: m.Endpoint, RemoteApp: m.RemoteApp,
			Suspended: r.Status.Suspended, Members: map[string]int64{}}
		if r.DeletionTimestamp != nil {
			rs.Life = resolver.Dying
		}
		for _, ud := range units[m.RemoteApp] {
			name := unitNameOf(ud.Name)
			if name == a.cfg.Unit {
				continue
			}
			st, in := ud.Spec.Relations[relKey(id)]
			if !in || !st.InScope {
				continue
			}
			rs.Members[name] = settingsVersion(st.Data)
			a.rememberSettings(id, name, st.Data)
		}
		if ad := w.appData[m.RemoteApp]; ad != nil {
			rs.AppVersion = settingsVersion(ad.Spec.Relations[relKey(id)])
		}
		out[id] = rs
	}
	// Relations the unit is in but that are gone (removed with the application): they are dying, so that the unit
	// departs from them (the endpoint and remote application come from the unit's recorded state after a restart).
	for id, st := range a.local.Relations {
		if _, ok := out[id]; ok || !st.InScope {
			continue
		}
		m, ok := a.relMeta[id]
		if !ok && st.Endpoint != "" {
			m, ok = relationMeta{Endpoint: st.Endpoint, RemoteApp: st.RemoteApp, Peer: st.RemoteApp == a.cfg.App}, true
		}
		if ok {
			out[id] = resolver.RelationSnapshot{Life: resolver.Dying, Peer: m.Peer, Endpoint: m.Endpoint, RemoteApp: m.RemoteApp}
		}
	}
	return out
}

func (a *Agent) rememberSettings(id int, unit string, data map[string]string) {
	if a.settingsSeen == nil {
		a.settingsSeen = map[string]map[string]string{}
	}
	a.settingsSeen[relKey(id)+"/"+unit] = data
}

// charmIdentity is the charm the unit's pod runs: the image the operator put in the pod's environment, else the one
// in the Application's status. The revision is known when that is the status's image.
func (a *Agent) charmIdentity(app *v1alpha1.Application) (string, int) {
	st := app.Status.Charm
	statusImage := ""
	if st != nil {
		if statusImage = st.Image; statusImage == "" {
			statusImage = st.Sha256
		}
	}
	image, _ := a.cfg.Getenv(v1alpha1.CharmImageEnv)
	if image == "" {
		image = statusImage
	}
	if image == "" {
		return "", 0
	}
	if st != nil && image == statusImage {
		return image, st.Revision
	}
	return image, 0
}

// storageDir is where a storage is mounted in the charm container: its declared location, else under the data dir
// (what the operator mounts, as juju does).
func (a *Agent) storageDir(name string) string {
	if def, ok := a.charm.Storage[name]; ok && def.Location != "" {
		return def.Location
	}
	return filepath.Join(a.cfg.DataDir, "storage", name, "0")
}

// storageID is the id of the unit's instance of a storage: <name>/<ordinal>, as juju numbers instances of a new application's
// storage; the volume is mounted at its location, else <data dir>/storage/<name>/0, in the charm container.
func (a *Agent) storageID(name string) string { return name + "/" + strconv.Itoa(a.cfg.Ordinal) }

// storageSnapshots lists the unit's storage instances (filesystem storage, one each) and whether they are mounted.
func (a *Agent) storageSnapshots() map[string]resolver.StorageSnapshot {
	out := map[string]resolver.StorageSnapshot{}
	for name, def := range a.charm.Storage {
		if def.Type != "" && def.Type != "filesystem" {
			continue
		}
		fi, err := os.Stat(a.storageDir(name))
		out[a.storageID(name)] = resolver.StorageSnapshot{Life: resolver.Alive, Attached: err == nil && fi.IsDir()}
	}
	return out
}

// ownedEntry is an indexed secret with its owner.
type ownedEntry struct {
	v1alpha1.OwnedSecret
	OwnerKind string
	OwnerID   string
}

// findOwned looks a secret up in the owner indexes: the application's, then each unit's of this application, then
// those of the other applications of the model (secrets shared with this application by a grant).
func (a *Agent) findOwned(w *world, xid string) (ownedEntry, bool) {
	if ad := w.appData[a.cfg.App]; ad != nil {
		if e, ok := ad.Spec.OwnedSecrets[xid]; ok {
			return ownedEntry{e, "application", a.cfg.App}, true
		}
	}
	if e, ok := a.store.Spec().OwnedSecrets[xid]; ok {
		return ownedEntry{e, "unit", a.cfg.Unit}, true
	}
	for i := range w.units {
		ud := &w.units[i]
		if appOfUnitData(ud) != a.cfg.App {
			continue
		}
		if e, ok := ud.Spec.OwnedSecrets[xid]; ok {
			return ownedEntry{e, "unit", unitNameOf(ud.Name)}, true
		}
	}
	apps := make([]string, 0, len(w.appData))
	for name := range w.appData {
		apps = append(apps, name)
	}
	sort.Strings(apps)
	for _, name := range apps {
		if name == a.cfg.App {
			continue
		}
		if e, ok := w.appData[name].Spec.OwnedSecrets[xid]; ok {
			return ownedEntry{e, "application", name}, true
		}
	}
	for i := range w.units {
		ud := &w.units[i]
		if appOfUnitData(ud) == a.cfg.App {
			continue
		}
		if e, ok := ud.Spec.OwnedSecrets[xid]; ok {
			return ownedEntry{e, "unit", unitNameOf(ud.Name)}, true
		}
	}
	return ownedEntry{}, false
}

// ownedSecrets are the secrets this unit manages: its own and, for the leader, the application's.
func (a *Agent) ownedSecrets(w *world) map[string]ownedEntry {
	out := map[string]ownedEntry{}
	for xid, e := range a.store.Spec().OwnedSecrets {
		out[xid] = ownedEntry{e, "unit", a.cfg.Unit}
	}
	if a.lead.IsLeader() {
		if ad := w.appData[a.cfg.App]; ad != nil {
			for xid, e := range ad.Spec.OwnedSecrets {
				out[xid] = ownedEntry{e, "application", a.cfg.App}
			}
		}
	}
	return out
}

// trackedRevisions are the revisions of a secret that units of the application track.
func (a *Agent) trackedRevisions(w *world, uri string) map[int]bool {
	out := map[int]bool{}
	if t, ok := a.store.Spec().TrackedSecrets[uri]; ok {
		out[t.Revision] = true
	}
	for i := range w.units {
		ud := &w.units[i]
		if unitNameOf(ud.Name) == a.cfg.Unit {
			continue
		}
		if t, ok := ud.Spec.TrackedSecrets[uri]; ok {
			out[t.Revision] = true
		}
	}
	return out
}

// secretSnapshots fills the snapshot's secret fields: new revisions of tracked secrets, obsolete and expired revisions
// of owned secrets, and tracked secrets that no longer exist.
func (a *Agent) secretSnapshots(ctx context.Context, w *world, snap *resolver.Snapshot) {
	spec := a.store.Spec()
	for uri, t := range spec.TrackedSecrets {
		xid := strings.TrimPrefix(uri, "secret:")
		e, ok := a.findOwned(w, xid)
		if !ok {
			// Not indexed (yet): ask the metadata Secret; a secret that is gone is deleted.
			s, err := a.kube.GetSecret(ctx, v1alpha1.SecretMetadataName(xid))
			switch {
			case apierrors.IsNotFound(err):
				if snap.DeletedSecretRevisions == nil {
					snap.DeletedSecretRevisions = map[string][]int{}
				}
				snap.DeletedSecretRevisions[uri] = nil
			case err == nil:
				m := metaFromSecret(s)
				e = ownedEntry{OwnedSecret: v1alpha1.OwnedSecret{Label: m.Label, Revision: m.Latest, Revisions: m.Revisions}}
				ok = true
			}
			if !ok {
				continue
			}
		}
		label := t.Label
		if label == "" {
			label = e.Label
		}
		if snap.ConsumedSecretInfo == nil {
			snap.ConsumedSecretInfo = map[string]resolver.ConsumedSecret{}
		}
		snap.ConsumedSecretInfo[uri] = resolver.ConsumedSecret{LatestRevision: e.Revision, Label: label}
	}
	now := a.clk.Now()
	for xid, e := range a.ownedSecrets(w) {
		uri := "secret:" + xid
		tracked := a.trackedRevisions(w, uri)
		for _, r := range e.Revisions {
			if r < e.Revision && !tracked[r] {
				if snap.ObsoleteSecretRevisions == nil {
					snap.ObsoleteSecretRevisions = map[string][]int{}
				}
				snap.ObsoleteSecretRevisions[uri] = append(snap.ObsoleteSecretRevisions[uri], r)
			}
		}
		if e.ExpireTime != nil && !e.ExpireTime.Time.After(now) {
			spec := resolver.SecretRevisionSpec(uri, e.Revision)
			if !a.expiredDone[spec] {
				snap.ExpiredSecretRevisions = append(snap.ExpiredSecretRevisions, spec)
			}
		}
	}
	for _, revs := range snap.ObsoleteSecretRevisions {
		sort.Ints(revs)
	}
	sort.Strings(snap.ExpiredSecretRevisions)
}

// nextSecretExpiry is when the next owned secret expires.
func (a *Agent) nextSecretExpiry() (time.Time, bool) {
	var next time.Time
	ok := false
	w := a.lastWorld
	if w == nil {
		w = &world{appData: map[string]*v1alpha1.AppData{}}
	}
	for xid, e := range a.ownedSecrets(w) {
		if e.ExpireTime == nil || a.expiredDone[resolver.SecretRevisionSpec("secret:"+xid, e.Revision)] {
			continue
		}
		if !ok || e.ExpireTime.Time.Before(next) {
			next, ok = e.ExpireTime.Time, true
		}
	}
	return next, ok
}

// refreshAddresses re-seeds the address keys of the unit's relation settings (the pod may have a new IP).
func (a *Agent) refreshAddresses(ctx context.Context) error {
	ip := a.cfg.PodIP()
	if ip == "" {
		return nil
	}
	spec := a.store.Spec()
	changed := false
	for _, r := range spec.Relations {
		if _, c := seedAddresses(r.Data, ip); r.InScope && c {
			changed = true
		}
	}
	if !changed {
		return nil
	}
	return a.store.Update(ctx, func(sp *v1alpha1.UnitDataSpec) {
		for k, r := range sp.Relations {
			if !r.InScope {
				continue
			}
			r.Data, _ = seedAddresses(r.Data, ip)
			sp.Relations[k] = r
		}
	})
}
