//go:build e2e

package e2e

import (
	"context"
	"strings"
	"testing"
	"time"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/luci1900/jk/api/v1alpha1"
)

// jkAs runs the CLI as another user (kubectl's impersonation).
func jkAs(t *testing.T, user, ns string, args ...string) (string, string, error) {
	t.Helper()
	return jk(t, ns, append([]string{"--as", user}, args...)...)
}

func bindUser(t *testing.T, ns, user, clusterRole string) {
	t.Helper()
	rb := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: user + "-" + clusterRole},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: clusterRole},
		Subjects:   []rbacv1.Subject{{Kind: "User", Name: user, APIGroup: "rbac.authorization.k8s.io"}},
	}
	if err := kube.Create(context.Background(), rb); err != nil {
		t.Fatal(err)
	}
}

// The offering model has the provider, the consuming model the client; the client relates to the offer through the CLI.
func TestCLIOffers(t *testing.T) {
	t.Parallel()
	offering, consumer := cliModel(t), cliModel(t)

	jkOK(t, offering, "deploy", cliCharm(t, "jk-test.charm"), "--config", "grant-data-secret=false")
	jkOK(t, consumer, "deploy", cliCharm(t, "jk-test-client.charm"))
	waitCLIActive(t, offering, "jk-test", 1, "greeting")
	waitCLIActive(t, consumer, "jk-test-client", 1, "providers: 0")

	// Offering: only the offered endpoints, only to the named models.
	if _, errOut, err := jk(t, offering, "offer", "jk-test:nope"); err == nil || !strings.Contains(errOut, `no endpoint "nope"`) {
		t.Fatalf("offer of a missing endpoint: %v %q", err, errOut)
	}
	if _, errOut, err := jk(t, offering, "offer", "jk-test:cluster"); err == nil || !strings.Contains(errOut, "peer endpoint") {
		t.Fatalf("offer of a peer endpoint: %v %q", err, errOut)
	}
	out := jkOK(t, offering, "offer", "jk-test:data")
	if !strings.Contains(out, `available at "`+offering+`.jk-test"`) {
		t.Fatalf("offer: %s", out)
	}
	url := offering + ".jk-test"
	if out := jkOK(t, offering, "offers"); !strings.Contains(out, url) || !strings.Contains(out, "data") {
		t.Fatalf("offers:\n%s", out)
	}
	if out := jkOK(t, consumer, "show-offer", url); !strings.Contains(out, "interface: jk_test_data") || !strings.Contains(out, "role: provider") {
		t.Fatalf("show-offer:\n%s", out)
	}

	// A user who may write in the consuming model, and read the offer, but whose model the offer does not allow.
	bindUser(t, consumer, "alice", "jk-writer")
	bindUser(t, offering, "alice", "jk-reader")
	_, errOut, err := jkAs(t, "alice", consumer, "integrate", "jk-test-client", url)
	if err == nil || !strings.Contains(errOut, "needs an offer there that allows this namespace") {
		t.Fatalf("alice before the offer allows the model: %v %q", err, errOut)
	}
	if out := jkOK(t, consumer, "status"); strings.Contains(out, "SAAS") {
		t.Fatalf("a refused relation left a trace:\n%s", out)
	}
	// The offer allows the model: the same user is let in (the operator publishes the pair a moment after the update).
	offer, err := (&e2eOffer{ns: offering, name: "jk-test"}).allow(consumer)
	if err != nil {
		t.Fatal(err)
	}
	_ = offer
	var integrated string
	eventually(t, 30*time.Second, "alice to be let in", func() (bool, error) {
		out, errOut, err := jkAs(t, "alice", consumer, "integrate", "jk-test-client", url)
		integrated = out
		if err != nil {
			return false, &cliError{errOut}
		}
		return true, nil
	})
	if !strings.Contains(integrated, "Integrated jk-test-client:source with "+url+":data") {
		t.Fatalf("integrate: %s", integrated)
	}

	// Data flows both ways through the operator.
	waitCLIActive(t, consumer, "jk-test-client", 1, "providers: 1")
	waitCLIActive(t, offering, "jk-test", 1, "clients: 1")
	text := jkOK(t, consumer, "status", "--relations")
	for _, want := range []string{"SAAS", "jk-test  joined", "local  " + url, "jk-test:data", "jk-test-client:source", "jk_test_data"} {
		if !strings.Contains(strings.Join(strings.Fields(text), " "), strings.Join(strings.Fields(want), " ")) {
			t.Errorf("consumer status lacks %q:\n%s", want, text)
		}
	}
	text = jkOK(t, offering, "status", "--relations")
	for _, want := range []string{"Offer", "jk-test  jk-test  data  1", "jk-test:data", ":source"} {
		if !strings.Contains(strings.Join(strings.Fields(text), " "), strings.Join(strings.Fields(want), " ")) {
			t.Errorf("offering status lacks %q:\n%s", want, text)
		}
	}
	if out := jkOK(t, offering, "show-offer", url); !strings.Contains(out, "application: jk-client") && !strings.Contains(out, "jk-test-client") {
		t.Fatalf("show-offer lacks the connection:\n%s", out)
	}
	// What the client wrote reaches the provider's side: the units' counts follow scaling on either side.
	jkOK(t, consumer, "add-unit", "jk-test-client")
	waitCLIActive(t, consumer, "jk-test-client", 2, "providers: 1")
	waitCLIActive(t, offering, "jk-test", 1, "clients: 2")
	jkOK(t, offering, "add-unit", "jk-test")
	waitCLIActive(t, offering, "jk-test", 2, "clients: 2")
	waitCLIActive(t, consumer, "jk-test-client", 2, "providers: 2")

	// An offer with connections is not removed without --force.
	if _, errOut, err := jk(t, offering, "remove-offer", "jk-test"); err == nil || !strings.Contains(errOut, "--force") {
		t.Fatalf("remove-offer: %v %q", err, errOut)
	}
	// The consumer removes its relation by the name it knows the remote application by; both sides run the hooks.
	jkOK(t, consumer, "remove-relation", "jk-test-client", "jk-test")
	waitCLIActive(t, consumer, "jk-test-client", 2, "providers: 0")
	waitCLIActive(t, offering, "jk-test", 2, "clients: 0")
	eventually(t, time.Minute, "the connection to be gone from the offer", func() (bool, error) {
		o, err := (&e2eOffer{ns: offering, name: "jk-test"}).get()
		if err != nil {
			return false, err
		}
		return len(o.Status.Connections) == 0, nil
	})
	var rels v1alpha1.RelationList
	if err := kube.List(context.Background(), &rels, client.InNamespace(offering)); err != nil {
		t.Fatal(err)
	}
	for _, r := range rels.Items {
		if r.Labels[v1alpha1.RemoteLabel] == "true" {
			t.Fatalf("the mirror %s is still in the offering model", r.Name)
		}
	}

	// Relate again under another name, then remove the offer with --force: the relation goes first, in order.
	jkOK(t, consumer, "integrate", "jk-test-client", url, "--alias", "provider")
	waitCLIActive(t, consumer, "jk-test-client", 2, "providers: 2")
	if out := jkOK(t, consumer, "status", "--relations"); !strings.Contains(out, "provider  joined") || !strings.Contains(out, "provider:data") {
		t.Fatalf("status with an alias:\n%s", out)
	}
	jkOK(t, offering, "remove-offer", "jk-test", "--force")
	waitCLIActive(t, consumer, "jk-test-client", 2, "providers: 0")
	waitCLIActive(t, offering, "jk-test", 2, "clients: 0")
	if out := jkOK(t, offering, "offers"); strings.Contains(out, url) {
		t.Fatalf("the offer is still listed:\n%s", out)
	}
	eventually(t, time.Minute, "the consumer's relation to go", func() (bool, error) {
		var rels v1alpha1.RelationList
		if err := kube.List(context.Background(), &rels, client.InNamespace(consumer)); err != nil {
			return false, err
		}
		for _, r := range rels.Items {
			if r.Spec.Offer != "" {
				return false, &cliError{r.Name}
			}
		}
		return true, nil
	})
}

type cliError struct{ msg string }

func (e *cliError) Error() string { return e.msg }

// e2eOffer reads and changes an Offer directly: the CLI has no command to change who may consume (juju's grant).
type e2eOffer struct{ ns, name string }

func (o *e2eOffer) get() (*v1alpha1.Offer, error) {
	var offer v1alpha1.Offer
	err := kube.Get(context.Background(), client.ObjectKey{Namespace: o.ns, Name: o.name}, &offer)
	return &offer, err
}

func (o *e2eOffer) allow(models ...string) (*v1alpha1.Offer, error) {
	offer, err := o.get()
	if err != nil {
		return nil, err
	}
	offer.Spec.AllowedModels = append(offer.Spec.AllowedModels, models...)
	return offer, kube.Update(context.Background(), offer)
}

func TestCLIOfferAllowedModelsFromModelConfig(t *testing.T) {
	t.Parallel()
	offering, consumer := cliModel(t), cliModel(t)
	jkOK(t, offering, "deploy", cliCharm(t, "jk-test.charm"))
	jkOK(t, offering, "model-config", "offer-allowed-models="+consumer)
	jkOK(t, offering, "offer", "jk-test:data", "shared")
	jkOK(t, consumer, "deploy", cliCharm(t, "jk-test-client.charm"))
	o, err := (&e2eOffer{ns: offering, name: "shared"}).get()
	if err != nil || len(o.Spec.AllowedModels) != 1 || o.Spec.AllowedModels[0] != consumer {
		t.Fatalf("allowed models %v %v", o, err)
	}
	// `--allow` overrides the default, and "*" lets every model in.
	jkOK(t, offering, "offer", "jk-test:data", "open", "--allow", "*")
	jkOK(t, consumer, "integrate", "jk-test-client", offering+".open")
	if out := jkOK(t, consumer, "status", "--relations"); !strings.Contains(out, "open  joined") {
		t.Fatalf("status:\n%s", out)
	}
}

// COS offers the endpoints that require things from the monitored model (prometheus's metrics-endpoint, grafana's
// dashboards): an offered endpoint can be the requiring side.
func TestCLIOfferOfARequirer(t *testing.T) {
	t.Parallel()
	offering, consumer := cliModel(t), cliModel(t)
	jkOK(t, offering, "deploy", cliCharm(t, "jk-test-client.charm"))
	jkOK(t, consumer, "deploy", cliCharm(t, "jk-test.charm"), "--config", "grant-data-secret=false")
	jkOK(t, offering, "offer", "jk-test-client:source", "wants-data", "--allow", consumer)
	if out := jkOK(t, consumer, "show-offer", offering+".wants-data"); !strings.Contains(out, "role: requirer") {
		t.Fatalf("show-offer:\n%s", out)
	}
	jkOK(t, consumer, "integrate", "jk-test", offering+".wants-data")
	waitCLIActive(t, consumer, "jk-test", 1, "clients: 1")
	waitCLIActive(t, offering, "jk-test-client", 1, "providers: 1")
	if out := jkOK(t, consumer, "status", "--relations"); !strings.Contains(out, "wants-data:source") {
		t.Fatalf("status:\n%s", out)
	}
	jkOK(t, consumer, "remove-relation", "jk-test", "wants-data")
	waitCLIActive(t, consumer, "jk-test", 1, "clients: 0")
	waitCLIActive(t, offering, "jk-test-client", 1, "providers: 0")
}
