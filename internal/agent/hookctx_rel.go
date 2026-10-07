package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sort"
	"time"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/internal/agent/resolver"
	"github.com/luci1900/jk/internal/hooktools"
)

func (h *hookContext) ModelUUID() string { return h.a.cfg.ModelUUID }

func (h *hookContext) HookRelation() (hooktools.HookRelation, bool) {
	if !h.info.IsRelation() {
		return hooktools.HookRelation{}, false
	}
	return hooktools.HookRelation{ID: h.info.RelationID, Endpoint: h.info.Endpoint, RemoteUnit: h.info.RemoteUnit, RemoteApp: h.info.RemoteApp}, true
}

func (h *hookContext) broken(id int) bool {
	return h.info.Kind == resolver.RelationBroken && h.info.RelationID == id
}

// RelationIDs lists the relations the unit is in, except the one a relation-broken hook is breaking.
func (h *hookContext) RelationIDs() []int {
	var ids []int
	for id, st := range h.local.Relations {
		if st.InScope && !h.broken(id) {
			ids = append(ids, id)
		}
	}
	sort.Ints(ids)
	return ids
}

// Relation returns a relation the unit is in. Its units are those joined so far (a departing unit is already gone in
// relation-departed).
func (h *hookContext) Relation(id int) (hooktools.RelationInfo, error) {
	st, ok := h.local.Relations[id]
	if !ok || !st.InScope {
		return hooktools.RelationInfo{}, hooktools.NotFoundf("relation")
	}
	m, ok := h.a.relMeta[id]
	if !ok && h.info.IsRelation() && h.info.RelationID == id {
		m, ok = relationMeta{Endpoint: h.info.Endpoint, RemoteApp: h.info.RemoteApp}, true
	}
	if !ok {
		return hooktools.RelationInfo{}, hooktools.NotFoundf("relation")
	}
	ri := hooktools.RelationInfo{ID: id, Endpoint: m.Endpoint, RemoteApp: m.RemoteApp, Units: []string{}}
	for u := range st.Members {
		if h.info.Kind == resolver.RelationDeparted && h.info.RelationID == id && u == h.info.RemoteUnit {
			continue
		}
		ri.Units = append(ri.Units, u)
	}
	// As in juju, the remote unit of a joined or changed hook is a member while the hook runs.
	if h.info.IsRelation() && h.info.RelationID == id && h.info.RemoteUnit != "" && h.info.Kind != resolver.RelationDeparted {
		if _, in := st.Members[h.info.RemoteUnit]; !in {
			ri.Units = append(ri.Units, h.info.RemoteUnit)
		}
	}
	sort.Strings(ri.Units)
	return ri, nil
}

func copyMap(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func applyKV(base map[string]string, kv map[string]string) map[string]string {
	out := copyMap(base)
	for k, v := range kv {
		if v == "" {
			delete(out, k)
		} else {
			out[k] = v
		}
	}
	return out
}

// ReadRelationSettings reads settings as the hook sees them: its own (and, for the leader, the application's) include the
// changes made so far; other units' come from the state at the start of the hook, falling back to the last seen
// settings for units that have left.
func (h *hookContext) ReadRelationSettings(id int, name string, app bool) (map[string]string, error) {
	if _, err := h.Relation(id); err != nil {
		return nil, err
	}
	if app {
		if name == h.a.cfg.App {
			var cur map[string]string
			if h.a.lead.IsLeader() {
				ad, err := h.a.kube.AppData(h.ctx)
				if err != nil {
					return nil, err
				}
				if ad != nil {
					cur = ad.Spec.Relations[relKey(id)]
				}
				return applyKV(cur, h.relApp[id]), nil
			}
			if ad := h.w.appData[name]; ad != nil {
				cur = ad.Spec.Relations[relKey(id)]
			}
			return copyMap(cur), nil
		}
		var cur map[string]string
		if ad := h.w.appData[name]; ad != nil {
			cur = ad.Spec.Relations[relKey(id)]
		}
		return copyMap(cur), nil
	}
	if name == h.a.cfg.Unit {
		return applyKV(h.a.store.Spec().Relations[relKey(id)].Data, h.relUnit[id]), nil
	}
	for i := range h.w.units {
		ud := &h.w.units[i]
		if unitNameOf(ud.Name) == name {
			if st, in := ud.Spec.Relations[relKey(id)]; in {
				return copyMap(st.Data), nil
			}
			break
		}
	}
	if seen, ok := h.a.settingsSeen[relKey(id)+"/"+name]; ok {
		return copyMap(seen), nil
	}
	return nil, hooktools.NotFoundf("settings for unit %q", name)
}

func (h *hookContext) SetRelationSettings(id int, app bool, kv map[string]string) error {
	if _, err := h.Relation(id); err != nil {
		return err
	}
	target := h.relUnit
	if app {
		if !h.a.lead.IsLeader() {
			return hooktools.ErrNotLeader
		}
		target = h.relApp
	}
	m := target[id]
	if m == nil {
		m = map[string]string{}
		target[id] = m
	}
	for k, v := range kv {
		m[k] = v
	}
	return nil
}

// applyRelationData merges the unit's relation settings changes into the UnitData spec.
func (h *hookContext) applyRelationData(sp *v1alpha1.UnitDataSpec) {
	for id, kv := range h.relUnit {
		k := relKey(id)
		st, ok := sp.Relations[k]
		if !ok {
			continue
		}
		st.Data = applyKV(st.Data, kv)
		if len(st.Data) == 0 {
			st.Data = nil
		}
		if sp.Relations == nil {
			sp.Relations = map[string]v1alpha1.UnitRelationState{}
		}
		sp.Relations[k] = st
	}
}

// commitAppData writes what only the leader may write: application relation settings and the index of application-owned
// secrets. idx carries the secret index changes made by commitSecrets.
func (h *hookContext) commitAppData(ctx context.Context, idx *secretIndexChanges) error {
	if len(h.relApp) == 0 && (idx == nil || !idx.appChanged()) {
		return nil
	}
	if !h.a.lead.IsLeader() {
		return hooktools.ErrNotLeader
	}
	mutate := func(sp *v1alpha1.AppDataSpec) {
		for id, kv := range h.relApp {
			k := relKey(id)
			data := applyKV(sp.Relations[k], kv)
			if sp.Relations == nil {
				sp.Relations = map[string]v1alpha1.RelationData{}
			}
			if len(data) == 0 {
				delete(sp.Relations, k)
			} else {
				sp.Relations[k] = data
			}
		}
		if idx != nil {
			idx.applyApp(sp)
		}
	}
	if ad, err := h.a.kube.AppData(ctx); err == nil {
		probe := v1alpha1.AppDataSpec{}
		if ad != nil {
			probe = *ad.Spec.DeepCopy()
		}
		mutate(&probe)
		if b, _ := json.Marshal(probe); len(b) > MaxObjectBytes {
			return fmt.Errorf("application data would be %d bytes, over the limit of %d", len(b), MaxObjectBytes)
		}
	}
	return h.a.kube.UpdateAppData(ctx, mutate)
}

// Storage.

func (h *hookContext) HookStorage() (string, bool) {
	if h.info.IsStorage() {
		return h.info.StorageID, true
	}
	return "", false
}

func (h *hookContext) StorageIDs() []string {
	var ids []string
	for id := range h.a.storageSnapshots() {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (h *hookContext) Storage(id string) (hooktools.StorageInfo, error) {
	name := resolver.StorageName(id)
	if _, ok := h.a.charm.Storage[name]; !ok || id != h.a.storageID(name) {
		return hooktools.StorageInfo{}, hooktools.NotFoundf("storage %q", id)
	}
	return hooktools.StorageInfo{ID: id, Kind: "filesystem", Location: h.a.storageDir(name)}, nil
}

// Network and cluster.

func (h *hookContext) BindingNames() []string {
	return append(h.a.charm.Endpoints(), h.a.charm.ExtraBindings...)
}

func (h *hookContext) NetworkInfo() hooktools.NetworkInfo {
	if h.a.cfg.Network != nil {
		return h.a.cfg.Network()
	}
	return interfaceInfo(h.a.cfg.PodIP())
}

// interfaceInfo finds the interface that carries ip.
func interfaceInfo(ip string) hooktools.NetworkInfo {
	info := hooktools.NetworkInfo{Address: ip}
	if ip == "" {
		return info
	}
	ifs, _ := net.Interfaces()
	for _, ifc := range ifs {
		addrs, _ := ifc.Addrs()
		for _, a := range addrs {
			if n, ok := a.(*net.IPNet); ok && n.IP.String() == ip {
				// ops parses the CIDR strictly, so it must be the network (10.244.0.0/24), not the address with a mask.
				info.Interface, info.MAC, info.CIDR = ifc.Name, ifc.HardwareAddr.String(), networkCIDR(n)
				return info
			}
		}
	}
	info.CIDR = ip + "/32"
	return info
}

// networkCIDR returns the network of an interface address, with the host bits cleared.
func networkCIDR(n *net.IPNet) string {
	return (&net.IPNet{IP: n.IP.Mask(n.Mask), Mask: n.Mask}).String()
}

// GoalState lists the application's units (those the Application wants plus those that exist) with their workload
// status; units being removed are "dying". Peer relations are not part of goal state, as in juju.
func (h *hookContext) GoalState() (hooktools.GoalState, error) {
	gs := hooktools.GoalState{Units: map[string]hooktools.GoalStatus{}, Relations: map[string]map[string]hooktools.GoalStatus{}}
	app := h.w.app
	scale := 1
	if app.Spec.Scale != nil {
		scale = int(*app.Spec.Scale)
	}
	deleting := app.DeletionTimestamp != nil
	epoch := time.Unix(0, 0)
	status := func(ud *v1alpha1.UnitData, ordinal int) hooktools.GoalStatus {
		st := hooktools.GoalStatus{Status: "allocating", Since: epoch}
		if ud != nil && ud.Spec.WorkloadStatus != nil && ud.Spec.WorkloadStatus.State != "" {
			st.Status = ud.Spec.WorkloadStatus.State
			if ud.Spec.WorkloadStatus.Since != nil {
				st.Since = ud.Spec.WorkloadStatus.Since.Time
			}
		}
		if deleting || ordinal >= scale {
			st.Status = "dying"
		}
		return st
	}
	byOrdinal := map[int]*v1alpha1.UnitData{}
	for i := range h.w.units {
		ud := &h.w.units[i]
		if appOfUnitData(ud) != h.a.cfg.App {
			continue
		}
		var n int
		if _, err := fmt.Sscanf(unitNameOf(ud.Name)[len(h.a.cfg.App)+1:], "%d", &n); err == nil {
			byOrdinal[n] = ud
		}
	}
	// This unit's own status is the live one.
	own := h.a.store.Spec()
	byOrdinal[h.a.cfg.Ordinal] = &v1alpha1.UnitData{Spec: own}
	for n := 0; n < scale; n++ {
		if _, ok := byOrdinal[n]; !ok {
			byOrdinal[n] = nil
		}
	}
	for n, ud := range byOrdinal {
		gs.Units[fmt.Sprintf("%s/%d", h.a.cfg.App, n)] = status(ud, n)
	}
	return gs, nil
}
