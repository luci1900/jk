// Package v1alpha1 contains the jk API types.
// +kubebuilder:object:generate=true
// +groupName=jk.luci1900.github.io
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

// Group is the API group, defined once; manifests are generated from the marker above.
const Group = "jk.luci1900.github.io"

var (
	GroupVersion  = schema.GroupVersion{Group: Group, Version: "v1alpha1"}
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}
	AddToScheme   = SchemeBuilder.AddToScheme
)
