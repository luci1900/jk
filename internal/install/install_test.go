package install

import (
	"strings"
	"testing"
)

func TestObjectsSubstituteImages(t *testing.T) {
	objs, err := Objects(Options{OperatorImage: "r/jk-operator:v1", AgentImage: "r/jk-agent:v1"})
	if err != nil {
		t.Fatal(err)
	}
	var deploy map[string]any
	for _, o := range objs {
		if o.GetKind() == "Deployment" && o.GetName() == "jk-operator" {
			deploy = o.Object
		}
	}
	if deploy == nil {
		t.Fatal("no operator Deployment")
	}
	var found int
	for _, o := range objs {
		b, _ := o.MarshalJSON()
		s := string(b)
		if strings.Contains(s, "JK_OPERATOR_IMAGE") || strings.Contains(s, "JK_AGENT_IMAGE_REF") {
			t.Errorf("%s %s still has a placeholder", o.GetKind(), o.GetName())
		}
		if o.GetKind() == "Deployment" && o.GetName() == "jk-operator" && strings.Contains(s, `"name":"JK_AGENT_IMAGE","value":"r/jk-agent:v1"`) && strings.Contains(s, `"image":"r/jk-operator:v1"`) {
			found++
		}
	}
	if found != 1 {
		t.Error("operator Deployment lacks JK_AGENT_IMAGE or its image")
	}
}

func TestOperatorRBACCoversM2(t *testing.T) {
	objs, err := Objects(Options{})
	if err != nil {
		t.Fatal(err)
	}
	var rules string
	for _, o := range objs {
		if o.GetKind() == "ClusterRole" && o.GetName() == "jk-operator" {
			b, _ := o.MarshalJSON()
			rules = string(b)
		}
	}
	for _, want := range []string{`"nodes"`, `"persistentvolumes"`, `"clusterrolebindings"`, `"relations/status"`, `"escalate"`} {
		if !strings.Contains(rules, want) {
			t.Errorf("operator ClusterRole lacks %s", want)
		}
	}
	for _, o := range objs {
		if o.GetKind() == "Deployment" && o.GetName() == "jk-operator" {
			b, _ := o.MarshalJSON()
			for _, want := range []string{"JK_REGISTRY_USERNAME", "JK_REGISTRY_PASSWORD", `"mountPath":"/tmp"`} {
				if !strings.Contains(string(b), want) {
					t.Errorf("operator Deployment lacks %s", want)
				}
			}
		}
	}
}
