package operator

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/luci1900/jk/api/v1alpha1"
)

// Reasons of the Valid condition on a Relation when it is false.
const (
	ReasonCrossNamespace    = "CrossNamespace"
	ReasonApplicationAbsent = "ApplicationNotFound"
	ReasonCharmPending      = "CharmNotResolved"
	ReasonEndpointNotFound  = "EndpointNotFound"
	ReasonSameApplication   = "SameApplication"
	ReasonIncompatible      = "IncompatibleEndpoints"
	ReasonInterfaceMismatch = "InterfaceMismatch"
	ReasonDuplicate         = "Duplicate"
	ReasonLimitExceeded     = "LimitExceeded"
	ReasonPeerRelation      = "PeerRelation"
	ReasonBadEndpoints      = "InvalidEndpoints"
	ReasonOfferMissing      = "OfferMissing"
	ReasonOfferNotFound     = "OfferNotFound"
	ReasonOfferEndpoint     = "OfferEndpoint"
	ReasonOfferNotReady     = "OfferNotReady"
	ReasonAlias             = "InvalidAlias"
	ReasonNotAModel         = "NotAModel"
)

// appKey keys applications by namespace and name, as endpoints of one relation can be in two namespaces.
func appKey(namespace, app string) string { return namespace + "/" + app }

// crossInfo is what validating a relation to another namespace needs besides its endpoints.
type crossInfo struct {
	// OfferName is spec.offer; Offer the Offer of that name in the other namespace (nil: there is none).
	OfferName string
	Offer     *v1alpha1.Offer
	// Alias is the name the remote application gets in the relation's namespace.
	Alias string
	// LocalApps are the applications of the relation's namespace and AliasUsed the aliases other relations there use.
	LocalApps map[string]bool
	AliasUsed map[string]bool
	// RemoteIsModel says whether the offering namespace is a jk model.
	RemoteIsModel bool
}

// aliasRule is a DNS label without hyphen-digit-only ambiguity concerns: lowercase letters, digits and hyphens.
var aliasRule = regexp.MustCompile(`^[a-z][a-z0-9]*(-[a-z0-9]+)*$`)

// relationApp is what validation knows about an endpoint's application: it exists, and its charm metadata once resolved.
type relationApp struct {
	Metadata *charmMetadata
}

// relationRef is another Relation of the namespace, as far as validation needs it.
type relationRef struct {
	Name      string
	Endpoints []v1alpha1.EndpointRef
	// ID is the assigned relation id (0 when none).
	ID      int64
	Created time.Time
}

// relationProblem says why a Relation is not valid.
type relationProblem struct {
	Reason, Message string
}

func (p *relationProblem) Error() string { return p.Reason + ": " + p.Message }

func problem(reason, format string, args ...any) *relationProblem {
	return &relationProblem{Reason: reason, Message: fmt.Sprintf(format, args...)}
}

// endpointRole is how a charm declares an endpoint.
type endpointRole string

const (
	roleProvides endpointRole = "provides"
	roleRequires endpointRole = "requires"
	rolePeer     endpointRole = "peers"
)

type endpointInfo struct {
	relationEndpoint
	Role endpointRole
}

func lookupEndpoint(md *charmMetadata, name string) (endpointInfo, bool) {
	if e, ok := md.Provides[name]; ok {
		return endpointInfo{e, roleProvides}, true
	}
	if e, ok := md.Requires[name]; ok {
		return endpointInfo{e, roleRequires}, true
	}
	if e, ok := md.Peers[name]; ok {
		return endpointInfo{e, rolePeer}, true
	}
	return endpointInfo{}, false
}

// endpointNames lists the endpoint names of the charm for error messages.
func endpointNames(md *charmMetadata) string {
	var names []string
	for _, m := range []map[string]relationEndpoint{md.Provides, md.Requires} {
		for n := range m {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	if len(names) == 0 {
		return "none"
	}
	return strings.Join(names, ", ")
}

// pairKey is the unordered identity of the endpoints of a relation.
func pairKey(eps []v1alpha1.EndpointRef) string {
	keys := make([]string, 0, len(eps))
	for _, e := range eps {
		keys = append(keys, e.Namespace+"/"+e.Application+":"+e.Endpoint)
	}
	sort.Strings(keys)
	return strings.Join(keys, " ")
}

// outranks says whether other should be kept in preference to a relation with the given id/age/name: relations that
// already have an id first, then the older, then by name.
func outranks(other relationRef, selfID int64, selfCreated time.Time, selfName string) bool {
	if (other.ID != 0) != (selfID != 0) {
		return other.ID != 0
	}
	if !other.Created.Equal(selfCreated) {
		return other.Created.Before(selfCreated)
	}
	return other.Name < selfName
}

// ValidateRelation checks a Relation against its applications' charms and the other Relations of the namespace.
// apps maps application name to what is known of it (absent: not found). A Relation that already has an id
// (selfID) is never held back by the duplicate and limit checks, which only decide between newcomers.
func ValidateRelation(namespace string, eps []v1alpha1.EndpointRef, peerLabelled bool, selfName string, selfID int64, selfCreated time.Time,
	apps map[string]relationApp, others []relationRef, cross *crossInfo) *relationProblem {
	if len(eps) == 0 || len(eps) > 2 {
		return problem(ReasonBadEndpoints, "a relation has one (peer) or two endpoints, not %d", len(eps))
	}
	if p := checkCross(namespace, eps, cross); p != nil {
		return p
	}
	infos := make([]endpointInfo, len(eps))
	for i, e := range eps {
		app, ok := apps[appKey(e.Namespace, e.Application)]
		if !ok {
			return problem(ReasonApplicationAbsent, "application %q does not exist", e.Application)
		}
		if app.Metadata == nil {
			return problem(ReasonCharmPending, "the charm of application %q is not resolved yet", e.Application)
		}
		info, ok := lookupEndpoint(app.Metadata, e.Endpoint)
		if !ok {
			return problem(ReasonEndpointNotFound, "application %q has no endpoint %q (endpoints: %s)", e.Application, e.Endpoint, endpointNames(app.Metadata))
		}
		infos[i] = info
	}
	if len(eps) == 1 {
		if infos[0].Role != rolePeer {
			return problem(ReasonBadEndpoints, "endpoint %s:%s is not a peer endpoint; a relation needs two endpoints", eps[0].Application, eps[0].Endpoint)
		}
		if !peerLabelled {
			return problem(ReasonPeerRelation, "peer relations are created by the operator for the charm's peers endpoints")
		}
	} else {
		if eps[0].Application == eps[1].Application && eps[0].Namespace == eps[1].Namespace {
			return problem(ReasonSameApplication, "cannot relate application %q to itself", eps[0].Application)
		}
		for i, in := range infos {
			if in.Role == rolePeer {
				return problem(ReasonIncompatible, "%s:%s is a peer endpoint", eps[i].Application, eps[i].Endpoint)
			}
		}
		if infos[0].Role == infos[1].Role {
			return problem(ReasonIncompatible, "%s:%s and %s:%s both %s: one side must provide and the other require",
				eps[0].Application, eps[0].Endpoint, eps[1].Application, eps[1].Endpoint, infos[0].Role)
		}
		if infos[0].Interface != infos[1].Interface {
			return problem(ReasonInterfaceMismatch, "%s:%s uses interface %q but %s:%s uses %q",
				eps[0].Application, eps[0].Endpoint, infos[0].Interface, eps[1].Application, eps[1].Endpoint, infos[1].Interface)
		}
	}

	key := pairKey(eps)
	for _, o := range others {
		if pairKey(o.Endpoints) == key && outranks(o, selfID, selfCreated, selfName) {
			return problem(ReasonDuplicate, "relation %q already relates %s", o.Name, strings.ReplaceAll(key, namespace+"/", ""))
		}
	}
	if selfID == 0 {
		for i, e := range eps {
			limit := int(infos[i].Limit)
			if limit == 0 {
				continue
			}
			n := 0
			for _, o := range others {
				if o.ID == 0 {
					continue
				}
				for _, oe := range o.Endpoints {
					if oe.Namespace == e.Namespace && oe.Application == e.Application && oe.Endpoint == e.Endpoint {
						n++
					}
				}
			}
			if n >= limit {
				return problem(ReasonLimitExceeded, "establishing a new relation for %s:%s would exceed its maximum relation limit of %d", e.Application, e.Endpoint, limit)
			}
		}
	}
	return nil
}

// checkCross validates what is particular to a relation across namespaces: it goes through an offer that exposes the
// remote endpoint, with a usable alias for the remote application. Access is checked by admission, not here.
func checkCross(namespace string, eps []v1alpha1.EndpointRef, cross *crossInfo) *relationProblem {
	other := ""
	for _, e := range eps {
		if e.Namespace != namespace {
			other = e.Namespace
		}
	}
	if other == "" {
		return nil
	}
	if len(eps) != 2 {
		return problem(ReasonBadEndpoints, "a relation has two endpoints to connect different namespaces")
	}
	local, remote := eps[0], eps[1]
	if local.Namespace != namespace {
		local, remote = remote, local
	}
	if local.Namespace != namespace || remote.Namespace == namespace {
		return problem(ReasonCrossNamespace, "a relation connects its own namespace %q with one other, not %q and %q", namespace, eps[0].Namespace, eps[1].Namespace)
	}
	if cross == nil || cross.OfferName == "" {
		return problem(ReasonOfferMissing, "a relation to namespace %q must name an offer in spec.offer", remote.Namespace)
	}
	if !cross.RemoteIsModel {
		return problem(ReasonNotAModel, "namespace %q is not a jk model", remote.Namespace)
	}
	if cross.Offer == nil {
		return problem(ReasonOfferNotFound, "offer %q not found in namespace %q", cross.OfferName, remote.Namespace)
	}
	offered := false
	for _, ep := range cross.Offer.Spec.Endpoints {
		offered = offered || ep == remote.Endpoint
	}
	if cross.Offer.Spec.Application != remote.Application || !offered {
		return problem(ReasonOfferEndpoint, "offer %q does not expose %s:%s (it offers %s: %s)", cross.OfferName, remote.Application, remote.Endpoint,
			cross.Offer.Spec.Application, strings.Join(cross.Offer.Spec.Endpoints, ", "))
	}
	if !aliasRule.MatchString(cross.Alias) || len(cross.Alias) > 50 {
		return problem(ReasonAlias, "invalid alias %q: lowercase letters, digits and hyphens, starting with a letter", cross.Alias)
	}
	if cross.LocalApps[cross.Alias] {
		return problem(ReasonAlias, "alias %q is the name of an application in namespace %q: choose another with spec.alias", cross.Alias, namespace)
	}
	if cross.AliasUsed[cross.Alias] {
		return problem(ReasonAlias, "alias %q is used by another relation in namespace %q: choose another with spec.alias", cross.Alias, namespace)
	}
	return nil
}

// relKey is the key of a relation id in UnitData.spec.relations.
func relKey(id int64) string { return strconv.FormatInt(id, 10) }

// UnitsInScope lists the units (ordinal names such as "app/0") of the given applications that still have the relation
// in scope: their relation-departed and relation-broken hooks have not finished. Units without a pod are left out,
// since nothing would ever clear their scope.
func UnitsInScope(id int64, apps []string, units []v1alpha1.UnitData, podExists func(name string) bool) []string {
	var out []string
	for i := range units {
		u := &units[i]
		app := ""
		for _, a := range apps {
			if _, ok := PodOrdinal(a, u.Name); ok {
				app = a
			}
		}
		if app == "" || !u.Spec.Relations[relKey(id)].InScope || !podExists(u.Name) {
			continue
		}
		n, _ := PodOrdinal(app, u.Name)
		out = append(out, fmt.Sprintf("%s/%d", app, n))
	}
	sort.Strings(out)
	return out
}
