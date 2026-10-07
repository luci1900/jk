package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// EndpointRef names one side of a relation. The namespace is part of the reference so
// offers can be added later; until then the operator rejects other namespaces.
type EndpointRef struct {
	Namespace   string `json:"namespace"`
	Application string `json:"application"`
	Endpoint    string `json:"endpoint"`
}

type RelationSpec struct {
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=2
	Endpoints []EndpointRef `json:"endpoints"`
	// Offer names the Offer (in the other endpoint's namespace) a relation across namespaces consumes. A Relation
	// is in the consumer's namespace; its endpoints name the consuming application and the offered one.
	Offer string `json:"offer,omitempty"`
	// Alias is the name the application on the other side has for the charms of this namespace (default: the
	// offer's name). It must not clash with an application of the namespace.
	Alias string `json:"alias,omitempty"`
}

type RelationStatus struct {
	// ID is the integer relation id charms see; assigned by the operator, never reused.
	ID int64 `json:"id,omitempty"`
	// RemoteID is, for a relation across namespaces, the id the other namespace gave it (each model numbers its own
	// relations, as in juju). The operator's mirror Relation in the offering namespace has it as its ID.
	RemoteID int64 `json:"remoteID,omitempty"`
	// Suspended mirrors juju's relation suspension.
	Suspended bool `json:"suspended,omitempty"`
	// ObservedGeneration is the spec generation the conditions describe.
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// Conditions: Ready summarises the others; Valid says whether the endpoints can be related (no id is assigned, so no hook runs, until it
	// is true); Removing is true while a deleted Relation waits for its units' relation-departed and
	// relation-broken hooks.
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// Relation condition types.
const (
	RelationValid    = "Valid"
	RelationRemoving = "Removing"
	// RelationReady is true once the relation is valid and has its id, and false while it is being removed.
	RelationReady = "Ready"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="ID",type=integer,JSONPath=`.status.id`
// +kubebuilder:printcolumn:name="Valid",type=string,JSONPath=`.status.conditions[?(@.type=="Valid")].status`

// Relation connects two application endpoints (or one, for a peer relation).
type Relation struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RelationSpec   `json:"spec"`
	Status RelationStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

type RelationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Relation `json:"items"`
}

func init() { SchemeBuilder.Register(&Relation{}, &RelationList{}) }
