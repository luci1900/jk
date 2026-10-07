// SPDX-License-Identifier: AGPL-3.0-only
//
// Derived from juju (https://github.com/juju/juju), AGPL-3.0: internal/worker/uniter/secrets/resolver.go and
// secrets/state.go. Rotation is not supported (no charm in the inventory uses it).

package resolver

import (
	"sort"
	"strconv"
	"strings"
)

// ConsumedSecret is what a consumer needs to know about a secret it tracks.
type ConsumedSecret struct {
	// LatestRevision is the newest revision.
	LatestRevision int
	// Label is the consumer's label (or the owner's, for secrets of the unit's own application).
	Label string
}

// SecretRevisionSpec formats "uri/revision" as the expired-revision list of the snapshot carries it.
func SecretRevisionSpec(uri string, rev int) string { return uri + "/" + strconv.Itoa(rev) }

func splitSecretRevisionSpec(s string) (string, int) {
	i := strings.LastIndexByte(s, '/')
	if i < 0 {
		return s, 0
	}
	rev, _ := strconv.Atoi(s[i+1:])
	return s[:i], rev
}

// secretsOp is juju's secrets resolver: secret-expired for owned secrets, secret-changed for consumed secrets with a newer
// revision, clean-up of secrets that were deleted, and secret-remove for owned revisions nobody tracks any more.
func secretsOp(local Local, remote Snapshot) (Operation, error) {
	if !local.Installed || remote.Life == Dying || local.Kind != Continue {
		return Operation{}, ErrNoOperation
	}
	for _, spec := range remote.ExpiredSecretRevisions {
		uri, rev := splitSecretRevisionSpec(spec)
		if rev == 0 {
			continue
		}
		return Operation{Kind: OpRunHook, Hook: HookInfo{Kind: SecretExpired, SecretURI: uri, SecretRevision: rev}}, nil
	}

	uris := make([]string, 0, len(remote.ConsumedSecretInfo))
	for uri := range remote.ConsumedSecretInfo {
		uris = append(uris, uri)
	}
	sort.Strings(uris)
	for _, uri := range uris {
		info := remote.ConsumedSecretInfo[uri]
		if existing := local.TrackedSecrets[uri]; existing != info.LatestRevision {
			return Operation{Kind: OpRunHook, Hook: HookInfo{Kind: SecretChanged, SecretURI: uri, SecretRevision: info.LatestRevision, SecretLabel: info.Label}}, nil
		}
	}

	gone := collectRemovedObsolete(local.ObsoleteSecretRevisions, remote.ObsoleteSecretRevisions)
	if len(remote.DeletedSecretRevisions) > 0 || len(gone) > 0 {
		return Operation{Kind: OpSecretsRemoved, DeletedSecrets: remote.DeletedSecretRevisions, DeletedObsolete: gone}, nil
	}

	uris = uris[:0]
	for uri := range remote.ObsoleteSecretRevisions {
		uris = append(uris, uri)
	}
	sort.Strings(uris)
	for _, uri := range uris {
		done := map[int]bool{}
		for _, r := range local.ObsoleteSecretRevisions[uri] {
			done[r] = true
		}
		revs := append([]int(nil), remote.ObsoleteSecretRevisions[uri]...)
		sort.Ints(revs)
		for _, rev := range revs {
			if !done[rev] {
				return Operation{Kind: OpRunHook, Hook: HookInfo{Kind: SecretRemove, SecretURI: uri, SecretRevision: rev}}, nil
			}
		}
	}
	return Operation{}, ErrNoOperation
}

// collectRemovedObsolete returns the processed-obsolete entries that no longer correspond to a known revision:
// secrets that are gone altogether have a nil slice (juju's CollectRemovedSecretObsoleteRevisions).
func collectRemovedObsolete(processed, known map[string][]int) map[string][]int {
	var out map[string][]int
	for uri, revs := range processed {
		knownRevs, ok := known[uri]
		if !ok {
			if out == nil {
				out = map[string][]int{}
			}
			out[uri] = nil
			continue
		}
		have := map[int]bool{}
		for _, r := range knownRevs {
			have[r] = true
		}
		var lost []int
		for _, r := range revs {
			if !have[r] {
				lost = append(lost, r)
			}
		}
		if len(lost) > 0 {
			if out == nil {
				out = map[string][]int{}
			}
			out[uri] = lost
		}
	}
	return out
}

// ApplySecretHook returns the tracked revisions and processed-obsolete revisions after a secret hook (juju's
// secrets State.UpdateStateForHook). The maps are copies.
func ApplySecretHook(tracked map[string]int, obsolete map[string][]int, h HookInfo) (map[string]int, map[string][]int) {
	t := make(map[string]int, len(tracked)+1)
	for k, v := range tracked {
		t[k] = v
	}
	o := make(map[string][]int, len(obsolete)+1)
	for k, v := range obsolete {
		o[k] = append([]int(nil), v...)
	}
	switch h.Kind {
	case SecretChanged:
		t[h.SecretURI] = h.SecretRevision
	case SecretRemove:
		revs := o[h.SecretURI]
		for _, r := range revs {
			if r == h.SecretRevision {
				return t, o
			}
		}
		revs = append(revs, h.SecretRevision)
		sort.Ints(revs)
		o[h.SecretURI] = revs
	}
	return t, o
}

// ApplySecretsRemoved prunes tracked and processed-obsolete revisions of secrets or revisions that were removed
// (juju's Secrets.SecretsRemoved). A nil revision list means the whole secret.
func ApplySecretsRemoved(tracked map[string]int, obsolete map[string][]int, deleted, deletedObsolete map[string][]int) (map[string]int, map[string][]int) {
	t := make(map[string]int, len(tracked))
	for k, v := range tracked {
		t[k] = v
	}
	o := make(map[string][]int, len(obsolete))
	for k, v := range obsolete {
		o[k] = append([]int(nil), v...)
	}
	prune := func(uri string, revs []int, dropTracked bool) {
		if len(revs) == 0 {
			if dropTracked {
				delete(t, uri)
			}
			delete(o, uri)
			return
		}
		gone := map[int]bool{}
		for _, r := range revs {
			gone[r] = true
		}
		var keep []int
		for _, r := range o[uri] {
			if !gone[r] {
				keep = append(keep, r)
			}
		}
		if len(keep) == 0 {
			delete(o, uri)
		} else {
			o[uri] = keep
		}
	}
	for uri, revs := range deleted {
		prune(uri, revs, true)
	}
	for uri, revs := range deletedObsolete {
		prune(uri, revs, false)
	}
	return t, o
}
