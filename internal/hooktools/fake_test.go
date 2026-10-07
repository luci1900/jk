// SPDX-License-Identifier: AGPL-3.0-only

package hooktools

import (
	"fmt"
	"sort"
	"time"
)

type fakeRel struct {
	RelationInfo
	settings map[string]map[string]string // unit name -> settings
	app      map[string]map[string]string // application name -> settings
}

type setCall struct {
	ID  int
	App bool
	KV  map[string]string
}

type getCall struct {
	URI, Label    string
	Refresh, Peek bool
}

// fakeCluster implements the relation, secret, storage and cluster parts of Backend.
type fakeCluster struct {
	unit, app string

	hookRel *HookRelation
	rels    map[int]*fakeRel
	broken  map[int]bool
	sets    []setCall

	secrets    map[string]SecretMetadata // by xid
	content    map[string]map[string][]byte
	secretLog  []string
	getCalls   []getCall
	created    []SecretCreate
	updates    map[string]SecretUpdate
	grants     map[string][]SecretGrant
	revoked    map[string][]SecretGrant
	removed    map[string][]int
	secretErr  error
	hookStor   string
	storage    map[string]StorageInfo
	bindings   []string
	goal       GoalState
	goalErr    error
	network    NetworkInfo
	modelUUID  string
	leaderFunc func() bool
}

func newFakeCluster() *fakeCluster {
	return &fakeCluster{unit: "app/0", app: "app", rels: map[int]*fakeRel{}, secrets: map[string]SecretMetadata{},
		content: map[string]map[string][]byte{}, updates: map[string]SecretUpdate{}, grants: map[string][]SecretGrant{},
		revoked: map[string][]SecretGrant{}, removed: map[string][]int{}, storage: map[string]StorageInfo{},
		modelUUID: "11111111-2222-3333-4444-555555555555",
		network:   NetworkInfo{Interface: "eth0", MAC: "aa:bb", Address: "10.1.2.3", CIDR: "10.1.2.3/32"}}
}

func (f *fakeCluster) addRel(id int, ep, remoteApp string, units ...string) *fakeRel {
	r := &fakeRel{RelationInfo: RelationInfo{ID: id, Endpoint: ep, RemoteApp: remoteApp, Units: units},
		settings: map[string]map[string]string{}, app: map[string]map[string]string{}}
	f.rels[id] = r
	return r
}

func (f *fakeCluster) HookRelation() (HookRelation, bool) {
	if f.hookRel == nil {
		return HookRelation{}, false
	}
	return *f.hookRel, true
}

func (f *fakeCluster) RelationIDs() []int {
	var ids []int
	for id := range f.rels {
		if !f.broken[id] {
			ids = append(ids, id)
		}
	}
	sort.Ints(ids)
	return ids
}

func (f *fakeCluster) Relation(id int) (RelationInfo, error) {
	r, ok := f.rels[id]
	if !ok {
		return RelationInfo{}, NotFoundf("relation")
	}
	return r.RelationInfo, nil
}

func (f *fakeCluster) ReadRelationSettings(id int, name string, app bool) (map[string]string, error) {
	r, ok := f.rels[id]
	if !ok {
		return nil, NotFoundf("relation")
	}
	m := r.settings
	if app {
		m = r.app
	}
	s, ok := m[name]
	if !ok {
		return nil, NotFoundf("settings for %q", name)
	}
	return s, nil
}

func (f *fakeCluster) SetRelationSettings(id int, app bool, kv map[string]string) error {
	f.sets = append(f.sets, setCall{id, app, kv})
	return nil
}

func (f *fakeCluster) ModelUUID() string { return f.modelUUID }

func (f *fakeCluster) CreateSecret(c SecretCreate) (string, error) {
	if f.secretErr != nil {
		return "", f.secretErr
	}
	f.created = append(f.created, c)
	return fmt.Sprintf("secret:cnj3q47mp25c7a0e7kl%d", len(f.created)-1+0), nil
}

func (f *fakeCluster) UpdateSecret(uri string, u SecretUpdate) error {
	f.updates[uri] = u
	return f.secretErr
}

func (f *fakeCluster) RemoveSecret(uri string, rev *int) error {
	if rev == nil {
		f.removed[uri] = nil
	} else {
		f.removed[uri] = append(f.removed[uri], *rev)
	}
	f.secretLog = append(f.secretLog, "remove "+uri)
	return f.secretErr
}

func (f *fakeCluster) GetSecret(uri, label string, refresh, peek bool) (map[string][]byte, error) {
	f.getCalls = append(f.getCalls, getCall{uri, label, refresh, peek})
	if f.secretErr != nil {
		return nil, f.secretErr
	}
	for xid, c := range f.content {
		if SecretURI(xid) == uri || (label != "" && f.secrets[xid].Label == label) {
			return c, nil
		}
	}
	if uri != "" {
		return nil, NotFoundf("secret %q", uri)
	}
	return nil, NotFoundf("secret with label %q", label)
}

func (f *fakeCluster) SecretMetadata() (map[string]SecretMetadata, error) {
	return f.secrets, f.secretErr
}

func (f *fakeCluster) GrantSecret(uri string, g SecretGrant) error {
	f.grants[uri] = append(f.grants[uri], g)
	return f.secretErr
}

func (f *fakeCluster) RevokeSecret(uri string, g SecretGrant) error {
	f.revoked[uri] = append(f.revoked[uri], g)
	return f.secretErr
}

func (f *fakeCluster) HookStorage() (string, bool) { return f.hookStor, f.hookStor != "" }

func (f *fakeCluster) StorageIDs() []string {
	var ids []string
	for id := range f.storage {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (f *fakeCluster) Storage(id string) (StorageInfo, error) {
	s, ok := f.storage[id]
	if !ok {
		return StorageInfo{}, NotFoundf("storage %q", id)
	}
	return s, nil
}

func (f *fakeCluster) NetworkInfo() NetworkInfo { return f.network }
func (f *fakeCluster) BindingNames() []string   { return f.bindings }
func (f *fakeCluster) GoalState() (GoalState, error) {
	return f.goal, f.goalErr
}

var _ = time.Now
