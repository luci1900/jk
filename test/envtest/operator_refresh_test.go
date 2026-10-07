package envtest

import (
	"context"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/internal/operator"
	"github.com/luci1900/jk/internal/registry"
)

// pushesOf counts the pushes of the named fake Charmhub charm (the fake store is shared by parallel tests).
func pushesOf(name string) int {
	n := 0
	for _, p := range opStore.pushed() {
		if strings.HasPrefix(string(p.Data), "PK-fake-charm-"+name) {
			n++
		}
	}
	return n
}

func charmOf(c client.Client, ns, name string) *v1alpha1.ResolvedCharm {
	if a := ready(c, ns, name); a != nil {
		return a.Status.Charm
	}
	return nil
}

func upToDate(c client.Client, ns, name string) *metav1.Condition {
	a := ready(c, ns, name)
	if a == nil {
		return nil
	}
	return meta.FindStatusCondition(a.Status.Conditions, operator.CharmUpToDateCondition)
}

func initImage(c client.Client, ns, name string) string {
	var sts appsv1.StatefulSet
	if get(c, ns, name, &sts) != nil {
		return ""
	}
	for _, a := range sts.Spec.Template.Spec.InitContainers[0].Args {
		if v, ok := strings.CutPrefix(a, "--charm-image="); ok {
			return v
		}
	}
	return ""
}

func TestCharmhubRefresh(t *testing.T) {
	t.Parallel()
	c := startOperator(t)
	ctx := context.Background()
	ns := newNamespace(t, c, true)
	hc := addPgish("rfsh")
	hc.channels = []string{"latest/stable", "latest/edge"}
	hc.chanRev = map[string]int{"latest/stable": 1, "latest/edge": 2}
	hc.actions = "echo:\n  params:\n    x: {type: string}\n"
	newHubApp(t, c, ns, "rfsh", nil)
	eventually(t, "revision 1", func() bool { ch := charmOf(c, ns, "rfsh"); return ch != nil && ch.Revision == 1 && ch.Image != "" })
	first := charmOf(c, ns, "rfsh")
	if first.Pin == "" || first.Actions == nil {
		t.Errorf("pin %q actions %v", first.Pin, first.Actions)
	}
	eventually(t, "up to date", func() bool {
		cond := upToDate(c, ns, "rfsh")
		return cond != nil && cond.Status == metav1.ConditionTrue
	})
	eventually(t, "image on the statefulset", func() bool { return initImage(c, ns, "rfsh") == first.Image })

	// A pod started with revision 1, and the charm's own rollout partition.
	var sts appsv1.StatefulSet
	_ = get(c, ns, "rfsh", &sts)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "rfsh-0", Namespace: ns, Labels: operator.PodLabels("rfsh"),
			Annotations: map[string]string{v1alpha1.CharmImageAnnotation: first.Image, v1alpha1.CharmRevisionAnnotation: "1"}},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "busybox"}}},
	}
	if err := c.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}
	patch := []byte(`{"spec":{"updateStrategy":{"type":"RollingUpdate","rollingUpdate":{"partition":1}}}}`)
	if err := c.Patch(ctx, &sts, client.RawPatch(types.MergePatchType, patch), client.FieldOwner("charm")); err != nil {
		t.Fatal(err)
	}
	eventually(t, "status.units shows the pod up to date", func() bool {
		u := ready(c, ns, "rfsh").Status.Units
		return len(u) == 1 && u[0].Name == "rfsh/0" && u[0].Revision == 1 && u[0].UpToDate
	})
	before, pushes := len(opHub.calls("rfsh")), pushesOf("rfsh")

	// Nothing re-resolves on its own.
	touch(t, c, ns, "rfsh")
	touch(t, c, ns, "rfsh")
	consistently(t, "resolved without a pin change", func() bool { return len(opHub.calls("rfsh")) == before })

	// A new channel is a refresh: resolved, downloaded, pushed, rolled out; the charm's partition survives.
	updateApp(t, c, ns, "rfsh", func(a *v1alpha1.Application) { a.Spec.Charm.Channel = "latest/edge" })
	eventually(t, "revision 2", func() bool {
		ch := charmOf(c, ns, "rfsh")
		return ch != nil && ch.Revision == 2 && ch.Image != first.Image
	})
	second := charmOf(c, ns, "rfsh")
	if len(opHub.calls("rfsh")) != before+2 || pushesOf("rfsh") != pushes+1 || second.Pin == first.Pin || second.Channel != "latest/edge" {
		t.Errorf("hub calls %d (was %d, a resolution is two requests), pushes %d (was %d), pin %q", len(opHub.calls("rfsh")), before, pushesOf("rfsh"), pushes, second.Pin)
	}
	eventually(t, "statefulset rolls to the new image", func() bool { return initImage(c, ns, "rfsh") == second.Image })
	_ = get(c, ns, "rfsh", &sts)
	if ru := sts.Spec.UpdateStrategy.RollingUpdate; ru == nil || ru.Partition == nil || *ru.Partition != 1 {
		t.Errorf("the charm's partition was changed: %+v", sts.Spec.UpdateStrategy)
	}
	if sts.Spec.Template.Annotations[v1alpha1.CharmRevisionAnnotation] != "2" {
		t.Errorf("template annotations %v", sts.Spec.Template.Annotations)
	}
	eventually(t, "status.units shows the pod still on revision 1", func() bool {
		u := ready(c, ns, "rfsh").Status.Units
		return len(u) == 1 && u[0].Revision == 1 && !u[0].UpToDate
	})

	// A refresh that cannot be done leaves the running charm and says so; fixing the pin clears it.
	updateApp(t, c, ns, "rfsh", func(a *v1alpha1.Application) { a.Spec.Charm.Channel = "nope/stable" })
	eventually(t, "CharmUpToDate false", func() bool {
		cond := upToDate(c, ns, "rfsh")
		return cond != nil && cond.Status == metav1.ConditionFalse && cond.Reason == "CharmNotFound" && strings.Contains(cond.Message, "latest/edge")
	})
	if ch := charmOf(c, ns, "rfsh"); ch.Revision != 2 || initImage(c, ns, "rfsh") != second.Image {
		t.Errorf("the running charm changed: %+v", ch)
	}
	if r := readyReason(c, ns, "rfsh"); r == "CharmNotFound" {
		t.Errorf("Ready reports the failed refresh: %s", r)
	}
	// Pinning the revision the edge channel already gave is the same file: bookkeeping only, no download.
	pushes = pushesOf("rfsh")
	updateApp(t, c, ns, "rfsh", func(a *v1alpha1.Application) { a.Spec.Charm.Channel = ""; r := 2; a.Spec.Charm.Revision = &r })
	eventually(t, "up to date again", func() bool {
		cond := upToDate(c, ns, "rfsh")
		return cond != nil && cond.Status == metav1.ConditionTrue && charmOf(c, ns, "rfsh").Pin != second.Pin
	})
	if ch := charmOf(c, ns, "rfsh"); ch.Image != second.Image || pushesOf("rfsh") != pushes {
		t.Errorf("the same revision was fetched again: %+v, %d pushes (was %d)", ch.Image, pushesOf("rfsh"), pushes)
	}
}

const digestSimple2 = "sha256:3333"

func TestLocalCharmRefresh(t *testing.T) {
	t.Parallel()
	opCharms.add(digestSimple2, &registry.Charm{
		Metadata: map[string]any{
			"name":       "simple2",
			"containers": map[string]any{"workload": map[string]any{"resource": "img"}},
			"resources":  map[string]any{"img": map[string]any{"type": "oci-image", "upstream-source": "busybox:2"}},
		},
		Base: registry.Base{Name: "ubuntu", Channel: "22.04"},
	})
	c := startOperator(t)
	ns := newNamespace(t, c, true)
	newApp(t, c, ns, "loc", digestSimple, 1)
	eventually(t, "first charm", func() bool {
		ch := charmOf(c, ns, "loc")
		return ch != nil && ch.Sha256 == digestSimple && ch.Pin != ""
	})
	first := charmOf(c, ns, "loc")
	eventually(t, "image on the statefulset", func() bool { return initImage(c, ns, "loc") == first.Image })

	updateApp(t, c, ns, "loc", func(a *v1alpha1.Application) { a.Spec.Charm.Sha256 = digestSimple2 })
	eventually(t, "second charm", func() bool { ch := charmOf(c, ns, "loc"); return ch != nil && ch.Sha256 == digestSimple2 })
	second := charmOf(c, ns, "loc")
	eventually(t, "statefulset rolls", func() bool { return initImage(c, ns, "loc") == second.Image })
	if second.Image == first.Image || second.Pin == first.Pin {
		t.Errorf("%+v %+v", first, second)
	}

	// A digest the registry lacks: the charm that runs stays, with a condition.
	updateApp(t, c, ns, "loc", func(a *v1alpha1.Application) { a.Spec.Charm.Sha256 = "sha256:missing" })
	eventually(t, "CharmUpToDate false", func() bool {
		cond := upToDate(c, ns, "loc")
		return cond != nil && cond.Status == metav1.ConditionFalse && cond.Reason == "CharmUnavailable"
	})
	if charmOf(c, ns, "loc").Sha256 != digestSimple2 || initImage(c, ns, "loc") != second.Image {
		t.Error("the running charm changed")
	}
	if r := readyReason(c, ns, "loc"); r == "CharmUnavailable" {
		t.Errorf("Ready reports the failed refresh")
	}
}
