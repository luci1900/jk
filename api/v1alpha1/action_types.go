package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// ActionExec is the name of the Action that implements `exec`.
const ActionExec = "juju-exec"

// ActionState is the lifecycle of a task.
// +kubebuilder:validation:Enum=pending;running;completed;failed;cancelled;aborting;aborted
type ActionState string

type ActionSpec struct {
	// Unit is the target, e.g. "postgresql-k8s/0".
	Unit string `json:"unit"`
	// Name is the action name, or "juju-exec".
	Name string `json:"name"`
	// +kubebuilder:pruning:PreserveUnknownFields
	// +kubebuilder:validation:Schemaless
	Parameters *JSON `json:"parameters,omitempty"`
	// Timeout in seconds; zero means no limit.
	TimeoutSeconds int32 `json:"timeoutSeconds,omitempty"`
}

type ActionStatus struct {
	// ID is the integer task id charms see in JUJU_ACTION_UUID.
	ID    int64       `json:"id,omitempty"`
	State ActionState `json:"state,omitempty"`
	// +kubebuilder:pruning:PreserveUnknownFields
	// +kubebuilder:validation:Schemaless
	Results   *JSON        `json:"results,omitempty"`
	Message   string       `json:"message,omitempty"`
	Log       []ActionLog  `json:"log,omitempty"`
	Started   *metav1.Time `json:"started,omitempty"`
	Completed *metav1.Time `json:"completed,omitempty"`
}

type ActionLog struct {
	Time    metav1.Time `json:"time"`
	Message string      `json:"message"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Unit",type=string,JSONPath=`.spec.unit`
// +kubebuilder:printcolumn:name="Action",type=string,JSONPath=`.spec.name`
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.state`

// Action is a juju task: one action run on one unit. The operation id is in the OperationLabel.
type Action struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ActionSpec   `json:"spec"`
	Status ActionStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

type ActionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Action `json:"items"`
}

func init() { SchemeBuilder.Register(&Action{}, &ActionList{}) }
