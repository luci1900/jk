package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// WorkloadStatus is a juju status: a state name, a message and a timestamp.
type WorkloadStatus struct {
	// State is one of juju's workload states (active, blocked, maintenance, waiting, unknown, error, ...).
	State   string       `json:"state"`
	Message string       `json:"message,omitempty"`
	Since   *metav1.Time `json:"since,omitempty"`
}
