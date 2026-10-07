package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// RelationData is a flat string map, as relation data is in juju.
type RelationData map[string]string

// UnitRelationState is what a unit has written and seen for one relation.
type UnitRelationState struct {
	// Data is the unit's own relation settings.
	Data RelationData `json:"data,omitempty"`
	// Members maps each remote unit to the settings version this unit last saw.
	Members map[string]int64 `json:"members,omitempty"`
	// ApplicationVersion is the app-data version this unit last saw.
	ApplicationVersion int64 `json:"applicationVersion,omitempty"`
	// InScope is true while the unit is part of the relation: other units list it as a member and read its Data.
	// A unit enters scope before relation-created and leaves it when its remove hook has run (or at relation-broken).
	InScope bool `json:"inScope,omitempty"`
	// Created is true once relation-created has run for this unit.
	Created bool `json:"created,omitempty"`
	// ChangedPending is the remote unit whose relation-changed must run next (juju queues it right after relation-joined).
	ChangedPending string `json:"changedPending,omitempty"`
	// Endpoint is the unit's endpoint of the relation and RemoteApp the application on the other side (the unit's own
	// application for a peer relation). They let a restarted agent depart from a Relation that has been deleted.
	Endpoint  string `json:"endpoint,omitempty"`
	RemoteApp string `json:"remoteApp,omitempty"`
}

// PortRange is an opened port range.
type PortRange struct {
	Protocol string `json:"protocol"`
	From     int32  `json:"from"`
	To       int32  `json:"to"`
}

// TrackedSecret is a consumer's tracked revision of a secret.
type TrackedSecret struct {
	Revision int    `json:"revision"`
	Label    string `json:"label,omitempty"`
}

// OwnedSecret is the index entry of a secret that the unit or the application owns. The metadata Secret is the source
// of truth; the index lets the agents find owned secrets by label, and see new revisions, without listing Secrets.
type OwnedSecret struct {
	Label string `json:"label,omitempty"`
	// Revision is the latest revision.
	Revision int `json:"revision"`
	// Revisions are the revisions that still exist.
	Revisions []int `json:"revisions,omitempty"`
	// ExpireTime is when the latest revision expires.
	ExpireTime *metav1.Time `json:"expireTime,omitempty"`
	// Grants mirrors the grants on the metadata Secret, so that consumers can tell from informer data whether they
	// may read the secret (and a Forbidden from the API server is a Role that has not caught up yet).
	Grants []SecretGrant `json:"grants,omitempty"`
}

// UnitDataSpec is written only by the unit's agent: everything a hook run commits.
type UnitDataSpec struct {
	// Operation is the uniter operation state (kind, step, hook, op id), opaque to the API.
	// +kubebuilder:pruning:PreserveUnknownFields
	// +kubebuilder:validation:Schemaless
	Operation *JSON `json:"operation,omitempty"`
	// ErrorState is set when a hook failed and awaits `resolved`.
	ErrorState *JSON `json:"errorState,omitempty"`
	// Relations maps relation id to this unit's state in it.
	Relations map[string]UnitRelationState `json:"relations,omitempty"`
	// State is the data set with state-set.
	State map[string]string `json:"state,omitempty"`
	// AgentStatus and WorkloadStatus as in juju status.
	AgentStatus     *WorkloadStatus          `json:"agentStatus,omitempty"`
	WorkloadStatus  *WorkloadStatus          `json:"workloadStatus,omitempty"`
	WorkloadVersion string                   `json:"workloadVersion,omitempty"`
	OpenedPorts     []PortRange              `json:"openedPorts,omitempty"`
	TrackedSecrets  map[string]TrackedSecret `json:"trackedSecrets,omitempty"`
	// ObsoleteSecretRevisions are the revisions of owned secrets for which secret-remove has run, by secret URI.
	ObsoleteSecretRevisions map[string][]int `json:"obsoleteSecretRevisions,omitempty"`
	// OwnedSecrets indexes the unit-owned secrets by secret id (the xid).
	OwnedSecrets map[string]OwnedSecret `json:"ownedSecrets,omitempty"`
	// StorageAttached maps storage id (name/index) to whether storage-attached has run (false once storage-detaching has).
	StorageAttached map[string]bool `json:"storageAttached,omitempty"`
	// CharmURL identifies the charm the unit last ran: the digest-pinned image reference of status.charm.image (else its sha256).
	CharmURL string `json:"charmURL,omitempty"`
	// CharmRevision is status.charm.revision of the charm the unit last ran.
	CharmRevision int `json:"charmRevision,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:printcolumn:name="Workload",type=string,JSONPath=`.spec.workloadStatus.state`
// +kubebuilder:printcolumn:name="Agent",type=string,JSONPath=`.spec.agentStatus.state`

// UnitData is one unit's state, named <app>-<n> and written only by that unit.
type UnitData struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec UnitDataSpec `json:"spec,omitempty"`
}

// +kubebuilder:object:root=true

type UnitDataList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []UnitData `json:"items"`
}

// AppDataSpec is written only by the leader.
type AppDataSpec struct {
	// Relations maps relation id to the application's relation settings.
	Relations map[string]RelationData `json:"relations,omitempty"`
	Status    *WorkloadStatus         `json:"status,omitempty"`
	// OwnedSecrets indexes the application-owned secrets by secret id (the xid).
	OwnedSecrets map[string]OwnedSecret `json:"ownedSecrets,omitempty"`
}

// +kubebuilder:object:root=true

// AppData is an application's leader-written state, named after the application.
type AppData struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec AppDataSpec `json:"spec,omitempty"`
}

// +kubebuilder:object:root=true

type AppDataList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AppData `json:"items"`
}

func init() {
	SchemeBuilder.Register(&UnitData{}, &UnitDataList{}, &AppData{}, &AppDataList{})
}
