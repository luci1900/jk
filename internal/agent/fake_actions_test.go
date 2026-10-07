package agent

import (
	"context"
	"sort"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/luci1900/jk/api/v1alpha1"
)

func (k *fakeKube) AppDatas(context.Context) ([]v1alpha1.AppData, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	var out []v1alpha1.AppData
	if k.appData != nil {
		out = append(out, v1alpha1.AppData{ObjectMeta: metav1.ObjectMeta{Name: "app"}, Spec: *k.appData.DeepCopy()})
	}
	for name, sp := range k.appDatas {
		out = append(out, v1alpha1.AppData{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: *sp.DeepCopy()})
	}
	return out, nil
}

func (k *fakeKube) Actions(context.Context) ([]v1alpha1.Action, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	var out []v1alpha1.Action
	for _, a := range k.actions {
		if a.Spec.Unit == "app/0" {
			out = append(out, *a.DeepCopy())
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (k *fakeKube) UpdateActionStatus(_ context.Context, name string, mutate func(*v1alpha1.Action) bool) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	a, ok := k.actions[name]
	if !ok {
		return nil
	}
	cp := a.DeepCopy()
	if mutate(cp) {
		k.actions[name] = cp
		k.actionWrites++
	}
	return nil
}

// addAction adds a pending Action for the test unit with the given task id.
func (k *fakeKube) addAction(name, action string, id int64, params string) *v1alpha1.Action {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.actions == nil {
		k.actions = map[string]*v1alpha1.Action{}
	}
	a := &v1alpha1.Action{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"},
		Spec:       v1alpha1.ActionSpec{Unit: "app/0", Name: action},
		Status:     v1alpha1.ActionStatus{ID: id, State: v1alpha1.ActionState("pending")},
	}
	if params != "" {
		a.Spec.Parameters = &v1alpha1.JSON{Raw: []byte(params)}
	}
	k.actions[name] = a
	return a
}

func (k *fakeKube) action(name string) v1alpha1.Action {
	k.mu.Lock()
	defer k.mu.Unlock()
	return *k.actions[name].DeepCopy()
}
