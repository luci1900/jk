// SPDX-License-Identifier: AGPL-3.0-only

package resolver

// Committed is the local state after a hook ran (or was skipped): the operation state moves on and the hook's effect on
// the relation, storage and secret bookkeeping is recorded. All of it is persisted together in one UnitData write.
func (l Local) Committed(h HookInfo, configHash string) Local {
	n := l
	n.State = l.State.Committed(h, configHash)
	switch {
	case h.IsRelation():
		rels := cloneRelations(l.Relations)
		if h.Kind == RelationBroken {
			// The relation is gone: the unit leaves its scope. The entry stays (out of scope, with the unit's last
			// settings) so that remote units can still read them while they depart.
			rels[h.RelationID] = RelationState{Endpoint: l.Relations[h.RelationID].Endpoint, RemoteApp: l.Relations[h.RelationID].RemoteApp}
		} else {
			rels[h.RelationID] = rels[h.RelationID].ApplyHook(h)
		}
		n.Relations = rels
	case h.IsStorage():
		n.StorageAttached = ApplyStorageHook(l.StorageAttached, h)
	case h.IsSecret():
		n.TrackedSecrets, n.ObsoleteSecretRevisions = ApplySecretHook(l.TrackedSecrets, l.ObsoleteSecretRevisions, h)
	case h.Kind == UpgradeCharm:
		n.CharmURL, n.CharmRevision = h.CharmURL, h.CharmRevision
	case h.Kind == Remove:
		// The unit is gone: other units see it depart.
		rels := cloneRelations(l.Relations)
		for id, st := range rels {
			st.InScope = false
			rels[id] = st
		}
		n.Relations = rels
	}
	return n
}

// CharmRecorded is the local state after the charm the unit runs was recorded without a hook.
func (l Local) CharmRecorded(url string, revision int) Local {
	n := l
	n.CharmURL, n.CharmRevision = url, revision
	return n
}

// EnteredScope is the local state after the unit entered the scope of a relation (endpoint and remoteApp as in the
// relation's snapshot).
func (l Local) EnteredScope(id int, endpoint, remoteApp string) Local {
	n := l
	rels := cloneRelations(l.Relations)
	st := rels[id]
	st.InScope = true
	st.Endpoint, st.RemoteApp = endpoint, remoteApp
	rels[id] = st
	n.Relations = rels
	return n
}

// SecretsRemoved is the local state after an OpSecretsRemoved.
func (l Local) SecretsRemoved(deleted, deletedObsolete map[string][]int) Local {
	n := l
	n.TrackedSecrets, n.ObsoleteSecretRevisions = ApplySecretsRemoved(l.TrackedSecrets, l.ObsoleteSecretRevisions, deleted, deletedObsolete)
	return n
}

func cloneRelations(in map[int]RelationState) map[int]RelationState {
	out := make(map[int]RelationState, len(in)+1)
	for k, v := range in {
		out[k] = v.clone()
	}
	return out
}
