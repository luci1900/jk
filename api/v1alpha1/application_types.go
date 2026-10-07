package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Trust is the level of k8s access granted to a charm.
// +kubebuilder:validation:Enum=none;namespace;cluster
type Trust string

const (
	TrustNone      Trust = "none"
	TrustNamespace Trust = "namespace"
	TrustCluster   Trust = "cluster"
)

// CharmSpec identifies a charm. Only Name is required; everything else is an optional pin
// that the operator resolves through Charmhub when unset (the result lands in status).
type CharmSpec struct {
	Name string `json:"name"`
	// +kubebuilder:validation:Enum=charmhub;local
	// +kubebuilder:default=charmhub
	Source  string `json:"source,omitempty"`
	Channel string `json:"channel,omitempty"`
	// +kubebuilder:validation:Minimum=0
	Revision *int   `json:"revision,omitempty"`
	Base     string `json:"base,omitempty"`
	URL      string `json:"url,omitempty"`
	Sha256   string `json:"sha256,omitempty"`
}

// StorageSpec configures one charm storage.
type StorageSpec struct {
	Size         string  `json:"size,omitempty"`
	StorageClass *string `json:"storageClass,omitempty"`
}

// Constraints are the subset of juju constraints supported on k8s.
type Constraints struct {
	Mem      string `json:"mem,omitempty"`
	CPUPower *int   `json:"cpu-power,omitempty"`
	Arch     string `json:"arch,omitempty"`
}

// ApplicationSpec is the desired state of a charmed application.
type ApplicationSpec struct {
	Charm CharmSpec `json:"charm"`
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=1
	Scale  *int32            `json:"scale,omitempty"`
	Config map[string]string `json:"config,omitempty"`
	// +kubebuilder:default=none
	Trust       Trust                  `json:"trust,omitempty"`
	Resources   map[string]string      `json:"resources,omitempty"`
	Storage     map[string]StorageSpec `json:"storage,omitempty"`
	Constraints *Constraints           `json:"constraints,omitempty"`
}

// ResolvedCharm is what the operator learned about the charm, recorded once.
type ResolvedCharm struct {
	Revision int `json:"revision,omitempty"`
	// Channel is the channel the revision was resolved from (Charmhub charms resolved by channel).
	Channel string `json:"channel,omitempty"`
	Base    string `json:"base,omitempty"`
	// Architecture is the architecture the charm and its pods run on (Charmhub charms).
	Architecture string `json:"architecture,omitempty"`
	URL          string `json:"url,omitempty"`
	// Sha256 is the hex sha256 of the downloaded .charm file for Charmhub charms, and the digest of the
	// charm image in jk-registry ("sha256:<hex>") for local charms.
	Sha256 string `json:"sha256,omitempty"`
	// Image is the digest-pinned reference of the charm in jk-registry.
	Image string `json:"image,omitempty"`
	// Metadata, Config and Actions are the charm's metadata.yaml, config.yaml and actions.yaml as JSON.
	// +kubebuilder:pruning:PreserveUnknownFields
	// +kubebuilder:validation:Schemaless
	Metadata *JSON `json:"metadata,omitempty"`
	// +kubebuilder:pruning:PreserveUnknownFields
	// +kubebuilder:validation:Schemaless
	ConfigSchema *JSON `json:"configSchema,omitempty"`
	// +kubebuilder:pruning:PreserveUnknownFields
	// +kubebuilder:validation:Schemaless
	Actions *JSON `json:"actions,omitempty"`
	// Pin identifies the spec pins (source, channel, revision, base, url, sha256, arch) this status was resolved from.
	// The operator resolves again only when the pins no longer match: changing them is a refresh.
	Pin string `json:"pin,omitempty"`
	// ResourceImages maps resource name to the image reference the pods use.
	ResourceImages map[string]string `json:"resourceImages,omitempty"`
}

// ApplicationStatus is the observed state of an application.
type ApplicationStatus struct {
	Charm              *ResolvedCharm     `json:"charm,omitempty"`
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
	// Leader is the unit holding leadership, e.g. "postgresql-k8s/0".
	Leader string `json:"leader,omitempty"`
	// Status is the application status as set by the leader.
	Status *WorkloadStatus `json:"status,omitempty"`
	// Units reports, for each existing pod, the charm it was started with.
	Units []UnitCharmStatus `json:"units,omitempty"`
}

// UnitCharmStatus is the charm a unit's pod runs; during a refresh units differ until each pod has rolled.
type UnitCharmStatus struct {
	// Name is the unit name, e.g. "postgresql-k8s/0".
	Name string `json:"name"`
	// Revision is the Charmhub revision of the charm the pod was started with (0 for local charms).
	Revision int `json:"revision,omitempty"`
	// Image is the charm image the pod was started with.
	Image string `json:"image,omitempty"`
	// UpToDate is true when Image is status.charm.image and the unit has run it (its upgrade-charm hook, if any, is done).
	UpToDate bool `json:"upToDate"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=app
// +kubebuilder:printcolumn:name="Charm",type=string,JSONPath=`.spec.charm.name`
// +kubebuilder:printcolumn:name="Rev",type=integer,JSONPath=`.status.charm.revision`
// +kubebuilder:printcolumn:name="Scale",type=integer,JSONPath=`.spec.scale`

// Application is a deployed charm.
type Application struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ApplicationSpec   `json:"spec"`
	Status ApplicationStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ApplicationList is a list of Applications.
type ApplicationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Application `json:"items"`
}

func init() { SchemeBuilder.Register(&Application{}, &ApplicationList{}) }
