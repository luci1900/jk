package sdk

import (
	"context"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/luci1900/jk/api/v1alpha1"
)

// OperatorInfo describes the installed operator.
type OperatorInfo struct {
	Image string
	// Version is the image's tag (empty for an image given by digest only).
	Version string
	Ready   int32
	Desired int32
}

// OperatorVersion reads the operator Deployment in jk-system.
func (c *Client) OperatorVersion(ctx context.Context) (*OperatorInfo, error) {
	var d appsv1.Deployment
	err := c.Kube.Get(ctx, client.ObjectKey{Namespace: v1alpha1.SystemNamespace, Name: "jk-operator"}, &d)
	if apierrors.IsNotFound(err) {
		return nil, ErrNotInstalled
	}
	if err != nil {
		return nil, err
	}
	info := &OperatorInfo{Ready: d.Status.ReadyReplicas}
	if d.Spec.Replicas != nil {
		info.Desired = *d.Spec.Replicas
	}
	if len(d.Spec.Template.Spec.Containers) > 0 {
		info.Image = d.Spec.Template.Spec.Containers[0].Image
	}
	info.Version = imageTag(info.Image)
	return info, nil
}

// imageTag is the tag of an image reference such as "ghcr.io/luci1900/jk-operator:v1", without any digest.
func imageTag(image string) string {
	image, _, _ = strings.Cut(image, "@")
	i := strings.LastIndex(image, ":")
	if i < 0 || strings.Contains(image[i:], "/") {
		return ""
	}
	return image[i+1:]
}
