// SPDX-License-Identifier: AGPL-3.0-only
//
// Derived from juju (https://github.com/juju/juju), AGPL-3.0:
// internal/provider/kubernetes/application/application.go (the StatefulSet, Service and pod layout).

package operator

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/internal/registry"
)

// errInvalid marks problems with the Application or charm that retrying won't fix.
var errInvalid = errors.New("invalid application")

const (
	nameLabel      = "app.kubernetes.io/name"
	managedByLabel = "app.kubernetes.io/managed-by"

	charmContainer   = "charm"
	initContainer    = "charm-init"
	dataVolume       = "charm-data"
	charmPebblePort  = 38812
	workloadPortBase = 38813
	healthProbePort  = 3856
	archLabel        = "kubernetes.io/arch"
	graceSeconds     = 30
)

// Config is the operator's own configuration.
type Config struct {
	// AgentImage is the charm-init image (jk-agent and pebble), from JK_AGENT_IMAGE.
	AgentImage string
	// RegistryEndpoint is where pods pull charms from (host:port).
	RegistryEndpoint string
	// CharmBaseRepo is the repository of the charm base images.
	CharmBaseRepo string
	// ClusterRetry is how soon an Application waiting on the cluster (mixed architectures, no nodes) looks again; 5s if zero.
	ClusterRetry time.Duration
}

// DefaultConfig fills the defaults for everything but the agent image.
func DefaultConfig(agentImage string) Config {
	return Config{AgentImage: agentImage, RegistryEndpoint: registry.InClusterEndpoint, CharmBaseRepo: "ghcr.io/juju/charm-base"}
}

// Desired is everything the operator applies for one Application (the Lease is handled separately, as agents
// write to it).
type Desired struct {
	ServiceAccount *corev1.ServiceAccount
	Role           *rbacv1.Role
	RoleBinding    *rbacv1.RoleBinding
	// SecretsRole and its binding grant the app's ServiceAccount access to the secrets it owns or was granted.
	SecretsRole        *rbacv1.Role
	SecretsRoleBinding *rbacv1.RoleBinding
	Service            *corev1.Service
	Endpoints          *corev1.Service
	StatefulSet        *appsv1.StatefulSet
	// PeerRelations has one Relation per peers: endpoint of the charm.
	PeerRelations []*v1alpha1.Relation
	// ClusterRole and ClusterRoleBinding exist for trust: cluster only. They are cluster-scoped, so they carry
	// labels instead of an owner reference and the Application finalizer deletes them.
	ClusterRole        *rbacv1.ClusterRole
	ClusterRoleBinding *rbacv1.ClusterRoleBinding
}

// Objects lists the namespaced objects in apply order.
func (d *Desired) Objects() []client.Object {
	out := []client.Object{d.ServiceAccount, d.Role, d.RoleBinding, d.SecretsRole, d.SecretsRoleBinding}
	for _, r := range d.PeerRelations {
		out = append(out, r)
	}
	return append(out, d.Endpoints, d.Service, d.StatefulSet)
}

// ClusterObjects lists the cluster-scoped objects (none unless trust is cluster).
func (d *Desired) ClusterObjects() []client.Object {
	if d.ClusterRole == nil {
		return nil
	}
	return []client.Object{d.ClusterRole, d.ClusterRoleBinding}
}

// Inputs are the observed facts Build needs besides the Application itself.
type Inputs struct {
	// NamespaceUID is the model UUID.
	NamespaceUID string
	// OpenedPorts is the union of the units' opened ports (see UnionPorts).
	OpenedPorts []v1alpha1.PortRange
	// OwnedSecrets and GrantedSecrets are the names of the Secrets (metadata and revisions) the application
	// owns and was granted, from SecretNames.
	OwnedSecrets, GrantedSecrets []string
}

// Labels are the labels on every object the operator creates for an application.
func Labels(app string) map[string]string {
	return map[string]string{nameLabel: app, v1alpha1.AppLabel: app, managedByLabel: "jk"}
}

// PodLabels are the labels on pods; the selector uses only the name label (immutable on a StatefulSet).
func PodLabels(app string) map[string]string {
	return map[string]string{nameLabel: app, v1alpha1.AppLabel: app}
}

// Build computes the desired objects. It is pure: it reads only its arguments. Needs status.charm to be set.
func Build(app *v1alpha1.Application, in Inputs, cfg Config) (*Desired, error) {
	namespaceUID := in.NamespaceUID
	ch := app.Status.Charm
	if ch == nil || ch.Image == "" {
		return nil, fmt.Errorf("%w: charm not resolved yet", errInvalid)
	}
	md, err := parseMetadata(ch.Metadata)
	if err != nil {
		return nil, err
	}
	if cfg.AgentImage == "" {
		return nil, fmt.Errorf("agent image not configured (JK_AGENT_IMAGE)")
	}
	name, ns := app.Name, app.Namespace
	owner := metav1.NewControllerRef(app, v1alpha1.GroupVersion.WithKind("Application"))
	meta := func(n string) metav1.ObjectMeta {
		return metav1.ObjectMeta{Name: n, Namespace: ns, Labels: Labels(name), OwnerReferences: []metav1.OwnerReference{*owner}}
	}

	d := &Desired{}
	d.ServiceAccount = &corev1.ServiceAccount{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ServiceAccount"},
		ObjectMeta: meta(name),
	}
	d.Role = &rbacv1.Role{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "Role"},
		ObjectMeta: meta(name),
		Rules:      append(AgentRules(), TrustRules(app.Spec.Trust, ns)...),
	}
	d.RoleBinding = &rbacv1.RoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
		ObjectMeta: meta(name),
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: name},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: name, Namespace: ns}},
	}
	d.Endpoints = &corev1.Service{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: meta(name + "-endpoints"),
		Spec: corev1.ServiceSpec{
			ClusterIP:                corev1.ClusterIPNone,
			PublishNotReadyAddresses: true,
			Selector:                 map[string]string{nameLabel: name},
		},
	}
	d.Service = &corev1.Service{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: meta(name),
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{nameLabel: name},
			Ports:    ServicePorts(in.OpenedPorts),
		},
	}
	d.SecretsRole = &rbacv1.Role{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "Role"},
		ObjectMeta: meta(SecretsRoleName(name)),
		Rules:      SecretRules(in.OwnedSecrets, in.GrantedSecrets),
	}
	d.SecretsRoleBinding = &rbacv1.RoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
		ObjectMeta: meta(SecretsRoleName(name)),
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: SecretsRoleName(name)},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: name, Namespace: ns}},
	}
	for _, ep := range sortedKeys(md.Peers) {
		d.PeerRelations = append(d.PeerRelations, &v1alpha1.Relation{
			TypeMeta:   metav1.TypeMeta{APIVersion: v1alpha1.GroupVersion.String(), Kind: "Relation"},
			ObjectMeta: peerMeta(meta(v1alpha1.PeerRelationName(name, ep)), ep),
			Spec:       v1alpha1.RelationSpec{Endpoints: []v1alpha1.EndpointRef{{Namespace: ns, Application: name, Endpoint: ep}}},
		})
	}
	if app.Spec.Trust == v1alpha1.TrustCluster {
		cn := ClusterObjectName(ns, name)
		cm := metav1.ObjectMeta{Name: cn, Labels: ClusterLabels(ns, name)}
		d.ClusterRole = &rbacv1.ClusterRole{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole"},
			ObjectMeta: cm, Rules: fullAccess(),
		}
		d.ClusterRoleBinding = &rbacv1.ClusterRoleBinding{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding"},
			ObjectMeta: cm,
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: cn},
			Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: name, Namespace: ns}},
		}
	}
	sts, err := buildStatefulSet(app, md, namespaceUID, cfg, meta(name))
	if err != nil {
		return nil, err
	}
	d.StatefulSet = sts
	return d, nil
}

// AgentRules are the permissions of the unit agent's Role (docs/design.md, Security).
func AgentRules() []rbacv1.PolicyRule {
	g := v1alpha1.Group
	return []rbacv1.PolicyRule{
		{APIGroups: []string{g}, Resources: []string{"applications", "appdata", "unitdata", "actions", "relations", "remotedata"}, Verbs: []string{"get", "list", "watch"}},
		{APIGroups: []string{g}, Resources: []string{"unitdata", "appdata"}, Verbs: []string{"create", "update", "patch"}},
		// Actions: the agent reports a task's progress and result in its status.
		{APIGroups: []string{g}, Resources: []string{"actions/status"}, Verbs: []string{"get", "update", "patch"}},
		{APIGroups: []string{""}, Resources: []string{"configmaps"}, Verbs: []string{"get", "list", "watch"}},
		{APIGroups: []string{"coordination.k8s.io"}, Resources: []string{"leases"}, Verbs: []string{"get", "list", "watch", "update"}},
		{APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"get", "patch"}},
		{APIGroups: []string{""}, Resources: []string{"events"}, Verbs: []string{"create"}},
		// Secrets: any unit may create them; reading and changing existing ones is granted by name in the
		// app's secrets Role, as RBAC cannot select by label.
		{APIGroups: []string{""}, Resources: []string{"secrets"}, Verbs: []string{"create"}},
	}
}

func envVar(name, value string) corev1.EnvVar { return corev1.EnvVar{Name: name, Value: value} }

func fieldEnv(name, path string) corev1.EnvVar {
	return corev1.EnvVar{Name: name, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: path}}}
}

// Readiness is probed every second, where juju uses 5s with one failure: a pod is Ready within about a second of its
// containers starting. Three failures keep a briefly slow Pebble from flapping the pod out of the Service endpoints.
const (
	readinessPeriod   int32 = 1
	readinessFailures int32 = 3
)

func httpProbe(port int, level string, period, failures int32) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler:     corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/v1/health?level=" + level, Port: intstr.FromInt32(int32(port))}},
		PeriodSeconds:    period,
		FailureThreshold: failures,
	}
}

func buildStatefulSet(app *v1alpha1.Application, md *charmMetadata, nsUID string, cfg Config, om metav1.ObjectMeta) (*appsv1.StatefulSet, error) {
	name, ns := app.Name, app.Namespace
	ch := app.Status.Charm

	containerNames := sortedKeys(md.Containers)
	identity := []corev1.EnvVar{
		fieldEnv("JK_POD_NAME", "metadata.name"),
		envVar("JK_NAMESPACE", ns),
		envVar("JK_MODEL_NAME", ns),
		envVar("JK_MODEL_UUID", nsUID),
		envVar("JK_APP", name),
		envVar(v1alpha1.CharmImageEnv, ch.Image),
	}
	if len(containerNames) > 0 { // charms such as data-integrator have no workload containers
		identity = append([]corev1.EnvVar{envVar("JUJU_CONTAINER_NAMES", strings.Join(containerNames, ","))}, identity...)
	}
	// Init and workload containers run as root explicitly, as juju does, so Pebble can switch to the layer's user whatever
	// the image's USER. The charm container deliberately sets nothing: its image runs as root anyway. Recent kubelets make
	// the service account token mode 0600 and owned by the runAsUser when every container sets the same one, which
	// stops a workload process running as another user (postgresql-k8s's Patroni) reading it; with one container
	// unset the token stays world-readable, as it was for juju.
	root := &corev1.SecurityContext{RunAsUser: ptr(int64(0)), RunAsGroup: ptr(int64(0))}

	// Storage: filesystem storages become volume claim templates (one volume per unit).
	var claims []corev1.PersistentVolumeClaim
	storages := sortedKeys(md.Storage)
	for _, s := range storages {
		st := md.Storage[s]
		if st.Type != "" && st.Type != "filesystem" {
			return nil, fmt.Errorf("%w: storage %q has type %q; only filesystem storage is supported", errInvalid, s, st.Type)
		}
		size := resource.MustParse("1Gi")
		if st.MinimumSize != "" {
			q, err := jujuSize(st.MinimumSize)
			if err != nil {
				return nil, fmt.Errorf("%w: storage %q minimum-size: %v", errInvalid, s, err)
			}
			size = q
		}
		var class *string
		if sp, ok := app.Spec.Storage[s]; ok {
			if sp.Size != "" {
				q, err := jujuSize(sp.Size)
				if err != nil {
					return nil, fmt.Errorf("%w: spec.storage.%s.size: %v", errInvalid, s, err)
				}
				size = q
			}
			class = sp.StorageClass
		}
		claims = append(claims, corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: volumeName(app, s), Labels: PodLabels(name)},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources:        corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: size}},
				StorageClassName: class,
			},
		})
	}

	sub := func(path string, ro bool) corev1.VolumeMount {
		return corev1.VolumeMount{Name: dataVolume, MountPath: "/" + path, SubPath: path, ReadOnly: ro}
	}
	initEnv := append([]corev1.EnvVar{}, identity...)
	charmEnv := append(append([]corev1.EnvVar{}, identity...), envVar("HTTP_PROBE_PORT", fmt.Sprint(healthProbePort)))
	// juju's mount at /var/lib/pebble/default is the containeragent/pebble dir.
	charmMounts := []corev1.VolumeMount{
		sub("charm/bin", true),
		sub("var/lib/juju", false),
		sub("charm/containers", false),
		{Name: dataVolume, MountPath: "/var/lib/pebble/default", SubPath: "containeragent/pebble"},
		{Name: dataVolume, MountPath: "/var/log/juju", SubPath: "containeragent/var/log/juju"},
	}
	for _, s := range storages {
		// juju mounts a filesystem in the charm container at the storage's declared location, else under /var/lib/juju/storage.
		// Charms such as postgresql-k8s write there themselves.
		path := md.Storage[s].Location
		if path == "" {
			path = "/var/lib/juju/storage/" + s + "/0"
		}
		charmMounts = append(charmMounts, corev1.VolumeMount{Name: volumeName(app, s), MountPath: path})
	}

	initC := corev1.Container{
		Name:            initContainer,
		Image:           cfg.AgentImage,
		ImagePullPolicy: corev1.PullIfNotPresent,
		WorkingDir:      "/var/lib/juju",
		Args: []string{
			"init",
			"--pebble-dir=/containeragent/pebble",
			"--data-dir=/var/lib/juju",
			"--bin-dir=/charm/bin",
			"--charm-image=" + ch.Image,
		},
		Env:             initEnv,
		SecurityContext: root,
		VolumeMounts: []corev1.VolumeMount{
			sub("var/lib/juju", false),
			sub("charm/bin", false),
			sub("charm/containers", false),
			sub("containeragent/pebble", false),
		},
	}
	base := &registry.Base{}
	base.Name, base.Channel, _ = strings.Cut(ch.Base, "@")
	charmC := corev1.Container{
		Name:            charmContainer,
		Image:           CharmBaseImage(cfg.CharmBaseRepo, *base),
		ImagePullPolicy: corev1.PullIfNotPresent,
		WorkingDir:      "/var/lib/juju",
		Command:         []string{"/charm/bin/pebble"},
		Args:            []string{"run", "--http", fmt.Sprintf(":%d", charmPebblePort), "--verbose"},
		Env:             charmEnv,
		StartupProbe:    httpProbe(charmPebblePort, "alive", 1, 30),
		LivenessProbe:   httpProbe(charmPebblePort, "alive", 5, 3),
		ReadinessProbe:  httpProbe(charmPebblePort, "ready", readinessPeriod, readinessFailures),
		VolumeMounts:    charmMounts,
	}

	containers := []corev1.Container{charmC}
	for i, cn := range containerNames {
		c := md.Containers[cn]
		img := ch.ResourceImages[c.Resource]
		if img == "" {
			return nil, fmt.Errorf("%w: container %q needs resource %q, which has no image (set spec.resources.%s)", errInvalid, cn, c.Resource, c.Resource)
		}
		port := workloadPortBase + i
		mounts := []corev1.VolumeMount{
			{Name: dataVolume, MountPath: "/charm/bin/pebble", SubPath: "charm/bin/pebble", ReadOnly: true},
			{Name: dataVolume, MountPath: "/charm/container", SubPath: "charm/containers/" + cn},
		}
		for _, m := range c.Mounts {
			if _, ok := md.Storage[m.Storage]; !ok {
				return nil, fmt.Errorf("%w: container %q mounts unknown storage %q", errInvalid, cn, m.Storage)
			}
			loc := m.Location
			if loc == "" {
				loc = md.Storage[m.Storage].Location
			}
			mounts = append(mounts, corev1.VolumeMount{Name: volumeName(app, m.Storage), MountPath: loc})
		}
		wc := corev1.Container{
			Name:            cn,
			Image:           img,
			ImagePullPolicy: corev1.PullIfNotPresent,
			Command:         []string{"/charm/bin/pebble"},
			Args:            []string{"run", "--create-dirs", "--hold", "--http", fmt.Sprintf(":%d", port), "--verbose"},
			Env: []corev1.EnvVar{
				envVar("JUJU_CONTAINER_NAME", cn),
				envVar("PEBBLE_SOCKET", "/charm/container/pebble.socket"),
				envVar("PEBBLE", "/charm/container/pebble"),
				envVar("PEBBLE_COPY_ONCE", "/var/lib/pebble/default"),
			},
			SecurityContext: root,
			StartupProbe:    httpProbe(port, "alive", 1, 30),
			LivenessProbe:   httpProbe(port, "alive", 5, 3),
			ReadinessProbe:  httpProbe(port, "ready", readinessPeriod, readinessFailures),
			VolumeMounts:    mounts,
		}
		if cons := app.Spec.Constraints; cons != nil {
			req := corev1.ResourceList{}
			if cons.Mem != "" {
				q, err := jujuSize(cons.Mem)
				if err != nil {
					return nil, fmt.Errorf("%w: constraints.mem: %v", errInvalid, err)
				}
				req[corev1.ResourceMemory] = q
			}
			if cons.CPUPower != nil {
				req[corev1.ResourceCPU] = *resource.NewMilliQuantity(int64(*cons.CPUPower), resource.DecimalSI)
			}
			if len(req) > 0 {
				wc.Resources.Requests = req
			}
		}
		containers = append(containers, wc)
	}

	var nodeSelector map[string]string
	if cons := app.Spec.Constraints; cons != nil && cons.Arch != "" {
		nodeSelector = map[string]string{archLabel: cons.Arch}
	} else if ch.Architecture != "" {
		nodeSelector = map[string]string{archLabel: ch.Architecture}
	}

	scale := int32(1)
	if app.Spec.Scale != nil {
		scale = *app.Spec.Scale
	}
	return &appsv1.StatefulSet{
		TypeMeta:   metav1.TypeMeta{APIVersion: "apps/v1", Kind: "StatefulSet"},
		ObjectMeta: om,
		Spec: appsv1.StatefulSetSpec{
			Replicas:            &scale,
			ServiceName:         name + "-endpoints",
			PodManagementPolicy: appsv1.ParallelPodManagement,
			Selector:            &metav1.LabelSelector{MatchLabels: map[string]string{nameLabel: name}},
			PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenScaled: appsv1.DeletePersistentVolumeClaimRetentionPolicyType, WhenDeleted: appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
			},
			VolumeClaimTemplates: claims,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: PodLabels(name), Annotations: map[string]string{
					v1alpha1.CharmImageAnnotation:    ch.Image,
					v1alpha1.CharmRevisionAnnotation: strconv.Itoa(ch.Revision),
				}},
				Spec: corev1.PodSpec{
					ServiceAccountName:            name,
					AutomountServiceAccountToken:  ptr(true),
					TerminationGracePeriodSeconds: ptr(int64(graceSeconds)),
					EnableServiceLinks:            ptr(false),
					NodeSelector:                  nodeSelector,
					Volumes:                       []corev1.Volume{{Name: dataVolume, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}},
					InitContainers:                []corev1.Container{initC},
					Containers:                    containers,
				},
			},
		},
	}, nil
}

func ptr[T any](v T) *T { return &v }
