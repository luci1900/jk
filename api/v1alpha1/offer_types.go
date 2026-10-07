package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// AllowAllModels in an Offer's allowedModels lets every model consume it.
const AllowAllModels = "*"

// OfferSpec says which endpoints of an application other models may relate to.
type OfferSpec struct {
	// Application is the offered application, in the Offer's namespace.
	Application string `json:"application"`
	// Endpoints are the offered endpoint names of the application.
	// +kubebuilder:validation:MinItems=1
	Endpoints []string `json:"endpoints"`
	// AllowedModels are the namespaces whose applications may relate to the offer ("*": all). A user who may create
	// relations in the Offer's namespace needs no entry.
	AllowedModels []string `json:"allowedModels,omitempty"`
}

// OfferConnection is a relation from another model to the offer.
type OfferConnection struct {
	// Namespace and Relation name the consumer's Relation; Application and Endpoint the consuming side.
	Namespace   string `json:"namespace"`
	Relation    string `json:"relation"`
	Application string `json:"application"`
	Endpoint    string `json:"endpoint"`
	// Status is "joining" until the relation is established, then "joined"; "removing" while it is being removed.
	Status string `json:"status"`
}

// OfferEndpoint describes an offered endpoint, so that consumers can pick the endpoints to relate without reading the
// offered application.
type OfferEndpoint struct {
	Name      string `json:"name"`
	Interface string `json:"interface"`
	// Role is "provider" or "requirer".
	Role string `json:"role"`
}

// OfferStatus is what the operator knows about the offer.
type OfferStatus struct {
	// Endpoints are the offered endpoints with their interfaces (set once the charm is resolved).
	Endpoints          []OfferEndpoint    `json:"endpoints,omitempty"`
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
	Connections        []OfferConnection  `json:"connections,omitempty"`
}

// Offer condition types.
const (
	// OfferReady is true while the offered application exists and has the offered endpoints.
	OfferReady = "Ready"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Application",type=string,JSONPath=`.spec.application`
// +kubebuilder:printcolumn:name="Endpoints",type=string,JSONPath=`.spec.endpoints`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`

// Offer exposes endpoints of an application to other models (namespaces).
type Offer struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   OfferSpec   `json:"spec"`
	Status OfferStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// OfferList is a list of Offers.
type OfferList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Offer `json:"items"`
}

// RemoteUnit is what a remote unit has published in a relation.
type RemoteUnit struct {
	Data RelationData `json:"data,omitempty"`
}

// RemoteDataSpec is the other side's data of a relation to another namespace, copied in by the operator (and only by
// it): what the application and its units on the other side have set in this one relation.
type RemoteDataSpec struct {
	// Application is the name charms see for the application on the other side (the alias), and Source the real one
	// as "<namespace>/<application>".
	Application string `json:"application"`
	Source      string `json:"source,omitempty"`
	// RelationID is the id of the relation in this namespace.
	RelationID int64 `json:"relationID"`
	// Units maps a remote unit name ("<alias>/<n>") to its settings, for the units in the relation.
	Units map[string]RemoteUnit `json:"units,omitempty"`
	// AppData are the remote application's settings.
	AppData RelationData `json:"appData,omitempty"`
	// Secrets maps the secrets the other side shares with this one (granted to it) to their latest revision. The
	// operator keeps copies of them in this namespace; the entry only tells the agents when something changed.
	Secrets map[string]int `json:"secrets,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:printcolumn:name="Remote",type=string,JSONPath=`.spec.application`
// +kubebuilder:printcolumn:name="Relation",type=integer,JSONPath=`.spec.relationID`

// RemoteData is named after the Relation of its namespace it belongs to.
type RemoteData struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec RemoteDataSpec `json:"spec,omitempty"`
}

// +kubebuilder:object:root=true

// RemoteDataList is a list of RemoteData.
type RemoteDataList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RemoteData `json:"items"`
}

func init() { SchemeBuilder.Register(&Offer{}, &OfferList{}, &RemoteData{}, &RemoteDataList{}) }
