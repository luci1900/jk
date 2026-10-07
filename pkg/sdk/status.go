package sdk

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/internal/version"
	"github.com/luci1900/jk/pkg/charmhub"
)

// StatusInfo is a juju status: a state, a message and when it was set.
type StatusInfo struct {
	Current string     `json:"current"`
	Message string     `json:"message,omitempty"`
	Since   *time.Time `json:"since,omitempty"`
}

// ModelStatus identifies the model.
type ModelStatus struct {
	Name      string    `json:"name"`
	Version   string    `json:"version"`
	Timestamp time.Time `json:"timestamp"`
}

// UnitStatus is one unit of an application.
type UnitStatus struct {
	WorkloadStatus  StatusInfo `json:"workload-status"`
	AgentStatus     StatusInfo `json:"juju-status"`
	WorkloadVersion string     `json:"workload-version,omitempty"`
	Leader          bool       `json:"leader,omitempty"`
	Address         string     `json:"address,omitempty"`
	OpenedPorts     []string   `json:"opened-ports,omitempty"`
	// CharmRevision is the revision the unit's pod runs (differs from the application's during a refresh).
	CharmRevision int `json:"charm-rev,omitempty"`
}

// ApplicationStatus is one application.
type ApplicationStatus struct {
	Charm        string                `json:"charm"`
	CharmOrigin  string                `json:"charm-origin"`
	CharmChannel string                `json:"charm-channel,omitempty"`
	CharmRev     int                   `json:"charm-rev,omitempty"`
	Base         string                `json:"base,omitempty"`
	Scale        int                   `json:"scale"`
	Version      string                `json:"version,omitempty"`
	Status       StatusInfo            `json:"application-status"`
	Address      string                `json:"address,omitempty"`
	Exposed      bool                  `json:"exposed"`
	Units        map[string]UnitStatus `json:"units,omitempty"`
}

// RelationStatus is one integration.
type RelationStatus struct {
	Provider  string `json:"provider"`
	Requirer  string `json:"requirer"`
	Interface string `json:"interface,omitempty"`
	// Type is "regular" or "peer".
	Type   string `json:"type"`
	Status string `json:"status"`
}

// OfferStatus is an offer of the model.
type OfferStatus struct {
	Application string                     `json:"application"`
	Endpoints   []v1alpha1.OfferEndpoint   `json:"endpoints,omitempty"`
	Allowed     []string                   `json:"allowed-models,omitempty"`
	Connections []v1alpha1.OfferConnection `json:"connections,omitempty"`
	// Ready is whether the offer can be consumed; Message says why not.
	Ready   bool   `json:"ready"`
	Message string `json:"message,omitempty"`
}

// RemoteApplicationStatus is an application of another model that this model's applications relate to (juju's SAAS).
type RemoteApplicationStatus struct {
	// Offer is the offer's URL, "<model>.<offer>".
	Offer string `json:"offer-url"`
	// Status is "joined", "joining", "removing" or "error".
	Status string `json:"status"`
	// Endpoints are the endpoints of the application related to the remote one.
	Endpoints []string `json:"endpoints,omitempty"`
	Message   string   `json:"message,omitempty"`
}

// Status is the state of a model.
type Status struct {
	Model              ModelStatus                        `json:"model"`
	Applications       map[string]ApplicationStatus       `json:"applications"`
	Offers             map[string]OfferStatus             `json:"offers,omitempty"`
	RemoteApplications map[string]RemoteApplicationStatus `json:"application-endpoints,omitempty"`
	Relations          []RelationStatus                   `json:"relations,omitempty"`
}

// severity orders workload states for an application's derived status (juju shows the most urgent one).
var severity = map[string]int{"error": 6, "blocked": 5, "waiting": 4, "maintenance": 3, "unknown": 2, "active": 1}

func since(t *metav1.Time) *time.Time {
	if t == nil {
		return nil
	}
	v := t.Time
	return &v
}

func statusInfo(s *v1alpha1.WorkloadStatus, def StatusInfo) StatusInfo {
	if s == nil || s.State == "" {
		return def
	}
	return StatusInfo{Current: s.State, Message: s.Message, Since: since(s.Since)}
}

// FormatPorts renders opened ports as juju does: "8065/TCP" or "8000-8010/TCP".
func FormatPorts(ports []v1alpha1.PortRange) []string {
	var out []string
	for _, p := range ports {
		proto := strings.ToUpper(p.Protocol)
		if p.From == p.To {
			out = append(out, fmt.Sprintf("%d/%s", p.From, proto))
		} else {
			out = append(out, fmt.Sprintf("%d-%d/%s", p.From, p.To, proto))
		}
	}
	return out
}

// Status reads the model's applications, units and relations.
func (c *Client) Status(ctx context.Context) (*Status, error) {
	if err := c.requireModel(ctx); err != nil {
		return nil, err
	}
	ns := client.InNamespace(c.Namespace)
	var apps v1alpha1.ApplicationList
	var units v1alpha1.UnitDataList
	var rels v1alpha1.RelationList
	var offers v1alpha1.OfferList
	var pods corev1.PodList
	var svcs corev1.ServiceList
	for _, l := range []struct {
		list client.ObjectList
		opts []client.ListOption
	}{{&apps, nil}, {&units, nil}, {&rels, nil}, {&offers, nil}, {&pods, nil}, {&svcs, nil}} {
		if err := c.Kube.List(ctx, l.list, append(l.opts, ns)...); err != nil {
			return nil, err
		}
	}
	st := &Status{
		Model:        ModelStatus{Name: c.Namespace, Version: version.Version, Timestamp: c.now().UTC()},
		Applications: map[string]ApplicationStatus{},
	}
	unitData := map[string]*v1alpha1.UnitData{}
	for i := range units.Items {
		unitData[units.Items[i].Name] = &units.Items[i]
	}
	podsByName := map[string]*corev1.Pod{}
	for i := range pods.Items {
		podsByName[pods.Items[i].Name] = &pods.Items[i]
	}
	addr := map[string]string{}
	for _, s := range svcs.Items {
		if s.Spec.ClusterIP != "" && s.Spec.ClusterIP != corev1.ClusterIPNone {
			addr[s.Name] = s.Spec.ClusterIP
		}
	}
	charms := map[string]*Charm{}
	for i := range apps.Items {
		app := &apps.Items[i]
		charms[app.Name], _ = CharmOf(app)
		st.Applications[app.Name] = applicationStatus(app, unitData, podsByName, addr[app.Name])
	}
	for i := range rels.Items {
		r := &rels.Items[i]
		st.Relations = append(st.Relations, relationStatus(r, charms))
		if remote := remoteSide(r); remote != nil && r.Labels[v1alpha1.RemoteLabel] != "true" {
			if st.RemoteApplications == nil {
				st.RemoteApplications = map[string]RemoteApplicationStatus{}
			}
			alias := aliasOfRelation(r)
			ra := st.RemoteApplications[alias]
			ra.Offer = OfferURL(remote.Namespace, r.Spec.Offer)
			ra.Status = relationStatusName(r)
			for _, e := range r.Spec.Endpoints {
				if e.Namespace == r.Namespace {
					ra.Endpoints = append(ra.Endpoints, e.Application+":"+e.Endpoint)
				}
			}
			if cond := meta.FindStatusCondition(r.Status.Conditions, v1alpha1.RelationReady); cond != nil && cond.Status != metav1.ConditionTrue {
				ra.Message = cond.Message
			}
			st.RemoteApplications[alias] = ra
		}
	}
	if len(offers.Items) > 0 {
		st.Offers = map[string]OfferStatus{}
	}
	for i := range offers.Items {
		o := &offers.Items[i]
		os := OfferStatus{Application: o.Spec.Application, Endpoints: o.Status.Endpoints, Allowed: o.Spec.AllowedModels, Connections: o.Status.Connections}
		if cond := meta.FindStatusCondition(o.Status.Conditions, v1alpha1.OfferReady); cond != nil {
			os.Ready, os.Message = cond.Status == metav1.ConditionTrue, cond.Message
			if os.Ready {
				os.Message = ""
			}
		}
		st.Offers[o.Name] = os
	}
	sort.Slice(st.Relations, func(i, j int) bool {
		return st.Relations[i].Provider+st.Relations[i].Requirer < st.Relations[j].Provider+st.Relations[j].Requirer
	})
	return st, nil
}

func applicationStatus(app *v1alpha1.Application, data map[string]*v1alpha1.UnitData, pods map[string]*corev1.Pod, address string) ApplicationStatus {
	scale := 1
	if app.Spec.Scale != nil {
		scale = int(*app.Spec.Scale)
	}
	out := ApplicationStatus{
		Charm: app.Spec.Charm.Name, CharmOrigin: "charmhub", Scale: scale, Address: address, Units: map[string]UnitStatus{},
	}
	if app.Spec.Charm.Source == "local" {
		out.CharmOrigin = "local"
	}
	if rc := app.Status.Charm; rc != nil {
		out.CharmChannel, out.CharmRev, out.Base = rc.Channel, rc.Revision, rc.Base
	}
	if out.CharmChannel == "" && out.CharmOrigin == "charmhub" {
		// A revision pin leaves no channel in status.charm: show the channel the application follows.
		out.CharmChannel = charmhub.NormalizeChannel(app.Spec.Charm.Channel)
	}
	// Units: every pod, every UnitData that is left, and the units the scale asks for.
	ordinals := map[int]bool{}
	prefix := app.Name + "-"
	collect := func(name string) {
		if rest, ok := strings.CutPrefix(name, prefix); ok {
			if n, err := strconv.Atoi(rest); err == nil && strconv.Itoa(n) == rest {
				ordinals[n] = true
			}
		}
	}
	for name := range pods {
		collect(name)
	}
	for name := range data {
		collect(name)
	}
	for n := 0; n < scale; n++ {
		ordinals[n] = true
	}
	var order []int
	for n := range ordinals {
		order = append(order, n)
	}
	sort.Ints(order)
	worst := ""
	var worstStatus StatusInfo
	for _, n := range order {
		unit := unitName(app.Name, n)
		pod := pods[podName(unit)]
		us := unitStatus(unit, data[podName(unit)], pod, app)
		out.Units[unit] = us
		if us.Leader && us.WorkloadVersion != "" {
			out.Version = us.WorkloadVersion
		}
		if severity[us.WorkloadStatus.Current] > severity[worst] {
			worst, worstStatus = us.WorkloadStatus.Current, us.WorkloadStatus
		}
	}
	if out.Version == "" {
		for _, n := range order {
			if v := out.Units[unitName(app.Name, n)].WorkloadVersion; v != "" {
				out.Version = v
				break
			}
		}
	}
	switch {
	case app.Status.Status != nil && app.Status.Status.State != "":
		out.Status = statusInfo(app.Status.Status, StatusInfo{})
	case worst != "":
		out.Status = worstStatus
	default:
		out.Status = StatusInfo{Current: "waiting", Message: "waiting for units"}
	}
	if len(out.Units) == 0 {
		out.Status = StatusInfo{Current: "waiting", Message: "waiting for units"}
	}
	return out
}

func unitStatus(unit string, ud *v1alpha1.UnitData, pod *corev1.Pod, app *v1alpha1.Application) UnitStatus {
	us := UnitStatus{
		WorkloadStatus: StatusInfo{Current: "waiting", Message: "waiting for pod"},
		AgentStatus:    StatusInfo{Current: "allocating"},
		Leader:         app.Status.Leader == unit,
	}
	if pod != nil {
		us.Address = pod.Status.PodIP
		us.WorkloadStatus.Message = "installing agent"
	}
	if ud != nil {
		sp := ud.Spec
		us.WorkloadStatus = statusInfo(sp.WorkloadStatus, us.WorkloadStatus)
		us.AgentStatus = statusInfo(sp.AgentStatus, StatusInfo{Current: "idle"})
		us.WorkloadVersion = sp.WorkloadVersion
		us.OpenedPorts = FormatPorts(sp.OpenedPorts)
		us.CharmRevision = sp.CharmRevision
	}
	if pod != nil && pod.DeletionTimestamp != nil {
		us.AgentStatus = StatusInfo{Current: "lost", Message: "pod is terminating"}
	}
	return us
}

// remoteSide is the endpoint of a relation that is in another namespace (nil for a relation within one).
func remoteSide(r *v1alpha1.Relation) *v1alpha1.EndpointRef {
	for i := range r.Spec.Endpoints {
		if r.Spec.Endpoints[i].Namespace != r.Namespace {
			return &r.Spec.Endpoints[i]
		}
	}
	return nil
}

func relationStatusName(r *v1alpha1.Relation) string {
	switch {
	case r.DeletionTimestamp != nil:
		return "removing"
	case meta.IsStatusConditionFalse(r.Status.Conditions, v1alpha1.RelationValid):
		return "error"
	case r.Status.Suspended:
		return "suspended"
	case !meta.IsStatusConditionTrue(r.Status.Conditions, v1alpha1.RelationReady):
		return "joining"
	}
	return "joined"
}

func relationStatus(r *v1alpha1.Relation, charms map[string]*Charm) RelationStatus {
	out := RelationStatus{Type: "regular", Status: relationStatusName(r)}
	eps := r.Spec.Endpoints
	// An application of another model is known by its alias.
	name := func(e v1alpha1.EndpointRef) string {
		if e.Namespace != r.Namespace {
			return aliasOfRelation(r)
		}
		return e.Application
	}
	ref := func(e v1alpha1.EndpointRef) string { return name(e) + ":" + e.Endpoint }
	switch len(eps) {
	case 1:
		out.Type, out.Provider, out.Requirer = "peer", ref(eps[0]), ref(eps[0])
		if ch := charms[eps[0].Application]; ch != nil {
			out.Interface = ch.Peers[eps[0].Endpoint].Interface
		}
	case 2:
		// The charm of the endpoint in this model tells which side provides.
		local, other := eps[0], eps[1]
		if local.Namespace != r.Namespace {
			local, other = other, local
		}
		prov, req := other, local
		if ch := charms[local.Application]; ch != nil {
			if e, ok := ch.Provides[local.Endpoint]; ok {
				prov, req = local, other
				out.Interface = e.Interface
			} else if e, ok := ch.Requires[local.Endpoint]; ok {
				out.Interface = e.Interface
			}
		}
		out.Provider, out.Requirer = ref(prov), ref(req)
	}
	return out
}
