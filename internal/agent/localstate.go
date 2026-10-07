package agent

import (
	"hash/fnv"
	"sort"
	"strconv"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/internal/agent/resolver"
)

func relKey(id int) string { return strconv.Itoa(id) }

// settingsVersion is the version of a relation settings bag as units compare it: 0 for no settings, otherwise a hash of
// the sorted pairs. Juju uses the settings document's revision; any value that changes with the content works, as the
// resolver compares versions with != (as juju does).
func settingsVersion(data map[string]string) int64 {
	if len(data) == 0 {
		return 0
	}
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := fnv.New64a()
	for _, k := range keys {
		h.Write([]byte(k))
		h.Write([]byte{0})
		h.Write([]byte(data[k]))
		h.Write([]byte{0})
	}
	v := int64(h.Sum64() & 0x7fffffffffffffff)
	if v == 0 {
		v = 1
	}
	return v
}

// localFromSpec builds the resolver's local state from the persisted UnitData.
func localFromSpec(sp v1alpha1.UnitDataSpec, st resolver.State) resolver.Local {
	l := resolver.Local{State: st, Relations: map[int]resolver.RelationState{}}
	for k, r := range sp.Relations {
		id, err := strconv.Atoi(k)
		if err != nil {
			continue
		}
		m := make(map[string]int64, len(r.Members))
		for u, v := range r.Members {
			m[u] = v
		}
		l.Relations[id] = resolver.RelationState{InScope: r.InScope, Created: r.Created, Members: m,
			AppVersion: r.ApplicationVersion, ChangedPending: r.ChangedPending, Endpoint: r.Endpoint, RemoteApp: r.RemoteApp}
	}
	l.CharmURL, l.CharmRevision = sp.CharmURL, sp.CharmRevision
	l.StorageAttached = map[string]bool{}
	for k, v := range sp.StorageAttached {
		l.StorageAttached[k] = v
	}
	l.TrackedSecrets = trackedFromSpec(sp)
	l.ObsoleteSecretRevisions = map[string][]int{}
	for k, v := range sp.ObsoleteSecretRevisions {
		l.ObsoleteSecretRevisions[k] = append([]int(nil), v...)
	}
	return l
}

func trackedFromSpec(sp v1alpha1.UnitDataSpec) map[string]int {
	m := map[string]int{}
	for k, v := range sp.TrackedSecrets {
		m[k] = v.Revision
	}
	return m
}

// applyLocal writes the resolver's bookkeeping into the UnitData spec, keeping what the resolver does not own (the
// unit's relation settings, the consumer labels of tracked secrets).
func applyLocal(sp *v1alpha1.UnitDataSpec, l resolver.Local) {
	rels := make(map[string]v1alpha1.UnitRelationState, len(l.Relations))
	for id, r := range l.Relations {
		old := sp.Relations[relKey(id)]
		m := make(map[string]int64, len(r.Members))
		for u, v := range r.Members {
			m[u] = v
		}
		if len(m) == 0 {
			m = nil
		}
		rels[relKey(id)] = v1alpha1.UnitRelationState{Data: old.Data, Members: m, ApplicationVersion: r.AppVersion,
			InScope: r.InScope, Created: r.Created, ChangedPending: r.ChangedPending, Endpoint: r.Endpoint, RemoteApp: r.RemoteApp}
	}
	sp.CharmURL, sp.CharmRevision = l.CharmURL, l.CharmRevision
	if len(rels) == 0 {
		rels = nil
	}
	sp.Relations = rels

	storage := map[string]bool{}
	for k, v := range l.StorageAttached {
		storage[k] = v
	}
	if len(storage) == 0 {
		storage = nil
	}
	sp.StorageAttached = storage

	tracked := map[string]v1alpha1.TrackedSecret{}
	for uri, rev := range l.TrackedSecrets {
		t := sp.TrackedSecrets[uri]
		t.Revision = rev
		tracked[uri] = t
	}
	if len(tracked) == 0 {
		tracked = nil
	}
	sp.TrackedSecrets = tracked

	obsolete := map[string][]int{}
	for k, v := range l.ObsoleteSecretRevisions {
		if len(v) > 0 {
			obsolete[k] = append([]int(nil), v...)
		}
	}
	if len(obsolete) == 0 {
		obsolete = nil
	}
	sp.ObsoleteSecretRevisions = obsolete
}

// addressSettings are the keys juju seeds a unit's relation settings with when it enters scope.
func addressSettings(ip string) map[string]string {
	if ip == "" {
		return nil
	}
	return map[string]string{"egress-subnets": ip + "/32", "ingress-address": ip, "private-address": ip}
}

// seedAddresses sets the address keys of data and reports whether anything changed.
func seedAddresses(data map[string]string, ip string) (map[string]string, bool) {
	seed := addressSettings(ip)
	if seed == nil {
		return data, false
	}
	out := make(map[string]string, len(data)+len(seed))
	for k, v := range data {
		out[k] = v
	}
	changed := false
	for k, v := range seed {
		if out[k] != v {
			out[k] = v
			changed = true
		}
	}
	return out, changed
}
