// Package install applies and removes jk's own manifests (CRDs, operator, registry, roles).
package install

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"sort"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/config"
)

const fieldOwner = "jk-install"

// Options configure an install.
type Options struct {
	// OperatorImage replaces the JK_OPERATOR_IMAGE placeholder.
	OperatorImage string
	// AgentImage replaces the JK_AGENT_IMAGE_REF placeholder: the charm-init image (jk-agent and pebble)
	// the operator puts in pods.
	AgentImage string
}

// Objects returns the manifests to apply, CRDs first, in a stable order.
func Objects(opts Options) ([]*unstructured.Unstructured, error) {
	var out []*unstructured.Unstructured
	for _, dir := range []string{"crd", "base"} {
		entries, err := fs.ReadDir(config.FS, dir)
		if err != nil {
			return nil, err
		}
		sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
		for _, e := range entries {
			data, err := fs.ReadFile(config.FS, dir+"/"+e.Name())
			if err != nil {
				return nil, err
			}
			text := strings.ReplaceAll(string(data), "JK_OPERATOR_IMAGE", opts.OperatorImage)
			data = []byte(strings.ReplaceAll(text, "JK_AGENT_IMAGE_REF", opts.AgentImage))
			dec := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)
			for {
				obj := &unstructured.Unstructured{}
				if err := dec.Decode(&obj.Object); err == io.EOF {
					break
				} else if err != nil {
					return nil, fmt.Errorf("%s/%s: %w", dir, e.Name(), err)
				}
				if len(obj.Object) == 0 {
					continue
				}
				out = append(out, obj)
			}
		}
	}
	return out, nil
}

// Apply server-side-applies all manifests; it is idempotent and also upgrades in place.
func Apply(ctx context.Context, c client.Client, opts Options) error {
	objs, err := Objects(opts)
	if err != nil {
		return err
	}
	for _, obj := range objs {
		if err := c.Patch(ctx, obj, client.Apply, client.FieldOwner(fieldOwner), client.ForceOwnership); err != nil {
			return fmt.Errorf("applying %s %s: %w", obj.GetKind(), obj.GetName(), err)
		}
		if obj.GetKind() == "Namespace" && obj.GetName() == v1alpha1.SystemNamespace {
			if err := ensureRegistryAuth(ctx, c); err != nil {
				return err
			}
		}
	}
	return nil
}

// Uninstall deletes jk-system and the CRDs (which removes all jk resources) and the cluster roles.
func Uninstall(ctx context.Context, c client.Client) error {
	objs, err := Objects(Options{})
	if err != nil {
		return err
	}
	// Reverse order: CRDs last would orphan nothing, but deleting the namespace first stops the operator.
	for i := len(objs) - 1; i >= 0; i-- {
		obj := objs[i]
		if obj.GetNamespace() != "" && obj.GetKind() != "Namespace" {
			continue // goes with the namespace
		}
		if err := client.IgnoreNotFound(c.Delete(ctx, obj)); err != nil {
			return fmt.Errorf("deleting %s %s: %w", obj.GetKind(), obj.GetName(), err)
		}
	}
	return nil
}

// pollInterval is how often WaitUninstalled looks again.
var pollInterval = time.Second

// WaitUninstalled waits until everything Uninstall deleted is gone. Deleting a CRD or the namespace returns at once
// but finishes later, and installing again meanwhile would apply onto an object that is going away.
func WaitUninstalled(ctx context.Context, c client.Client, timeout time.Duration) error {
	objs, err := Objects(Options{})
	if err != nil {
		return err
	}
	var left []string
	check := func(ctx context.Context) (bool, error) {
		left = left[:0]
		for _, obj := range objs {
			if obj.GetNamespace() != "" && obj.GetKind() != "Namespace" {
				continue // goes with the namespace
			}
			err := c.Get(ctx, client.ObjectKeyFromObject(obj), obj.DeepCopy())
			if apierrors.IsNotFound(err) {
				continue
			}
			if err != nil {
				return false, err
			}
			left = append(left, obj.GetKind()+" "+obj.GetName())
		}
		return len(left) == 0, nil
	}
	if err := wait.PollUntilContextTimeout(ctx, pollInterval, timeout, true, check); err != nil {
		if len(left) > 0 {
			return fmt.Errorf("timed out waiting for %s to be deleted", strings.Join(left, ", "))
		}
		return err
	}
	return nil
}

// RegistryAuthSecret holds the credentials for pushing to jk-registry (keys: username, password, htpasswd).
const RegistryAuthSecret = "jk-registry-auth"

// ensureRegistryAuth creates the registry credentials if missing. An existing Secret is never touched,
// so re-installing doesn't rotate the password.
func ensureRegistryAuth(ctx context.Context, c client.Client) error {
	key := client.ObjectKey{Namespace: v1alpha1.SystemNamespace, Name: RegistryAuthSecret}
	if err := c.Get(ctx, key, &corev1.Secret{}); err == nil {
		return nil
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	pw := make([]byte, 24)
	if _, err := rand.Read(pw); err != nil {
		return err
	}
	password := hex.EncodeToString(pw)
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name, Labels: map[string]string{"app.kubernetes.io/part-of": "jk"}},
		StringData: map[string]string{"username": "jk", "password": password, "htpasswd": "jk:" + string(hash) + "\n"},
	}
	return client.IgnoreAlreadyExists(c.Create(ctx, secret))
}
