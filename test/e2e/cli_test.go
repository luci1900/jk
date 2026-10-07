//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/luci1900/jk/pkg/sdk"
)

// The scenarios here drive the built CLI (bin/kubectl-jk, made by `make build`) the way a user does.

func cliBinary(t *testing.T) string {
	t.Helper()
	p, err := filepath.Abs("../../bin/kubectl-jk")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(p); err != nil {
		t.Fatalf("CLI missing (run `make build`): %v", err)
	}
	return p
}

// cliCharm returns the path of a test charm built by `make charm`.
func cliCharm(t *testing.T, file string) string {
	t.Helper()
	p, err := filepath.Abs(filepath.Join("../../bin", file))
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// jk runs the CLI against the cluster under test, in model ns, and returns stdout, stderr and the error.
func jk(t *testing.T, ns string, args ...string) (string, string, error) {
	t.Helper()
	full := []string{"--context", kubeContext}
	if ns != "" {
		full = append(full, "-m", ns)
	}
	full = append(full, args...)
	cmd := exec.Command(cliBinary(t), full...)
	var out, errOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errOut
	start := time.Now()
	err := cmd.Run()
	if err != nil || len(args) == 0 || (args[0] != "status" && args[0] != "ssh") { // status and ssh are polled: log only their failures
		t.Logf("jk %s (%s)", strings.Join(args, " "), time.Since(start).Round(time.Millisecond))
	}
	return out.String(), errOut.String(), err
}

// jkOK runs the CLI and fails the test if it fails.
func jkOK(t *testing.T, ns string, args ...string) string {
	t.Helper()
	out, errOut, err := jk(t, ns, args...)
	if err != nil {
		t.Fatalf("jk %s: %v\nstdout: %s\nstderr: %s", strings.Join(args, " "), err, out, errOut)
	}
	return out
}

// cliModel creates a model through the CLI (without switching the kubeconfig) and cleans up like newModel does.
func cliModel(t *testing.T) string {
	t.Helper()
	ns := fmt.Sprintf("e2e-%06x", rand.IntN(1<<24))
	jkOK(t, "", "add-model", ns, "--no-switch")
	if os.Getenv("JK_E2E_KEEP") != "" {
		return ns
	}
	modelsMu.Lock()
	models = append(models, ns)
	modelsMu.Unlock()
	t.Cleanup(func() {
		if t.Failed() {
			dump(t, ns)
		}
		_ = kube.Delete(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}})
	})
	return ns
}

// cliStatus reads `status --format json`.
func cliStatus(t *testing.T, ns string) sdk.Status {
	t.Helper()
	var st sdk.Status
	if err := json.Unmarshal([]byte(jkOK(t, ns, "status", "--format", "json")), &st); err != nil {
		t.Fatal(err)
	}
	return st
}

// waitCLIActive waits until the application has scale units, all active with the message containing msg.
func waitCLIActive(t *testing.T, ns, app string, scale int, msg string) {
	t.Helper()
	eventually(t, readyTimeout, fmt.Sprintf("%d units of %s active via status", scale, app), func() (bool, error) {
		a, ok := cliStatus(t, ns).Applications[app]
		if !ok {
			return false, fmt.Errorf("no application %s", app)
		}
		if len(a.Units) != scale {
			return false, fmt.Errorf("%d units", len(a.Units))
		}
		for name, u := range a.Units {
			if u.WorkloadStatus.Current != "active" || !strings.Contains(u.WorkloadStatus.Message, msg) {
				return false, fmt.Errorf("%s: %+v", name, u.WorkloadStatus)
			}
		}
		return true, nil
	})
}

func TestCLIDeployRelateRun(t *testing.T) {
	t.Parallel()
	ns := cliModel(t)

	// The model shows up, marked as current when it is the namespace of the context.
	if out := jkOK(t, ns, "models"); !strings.Contains(out, ns) || !strings.Contains(out, "available") {
		t.Fatalf("models:\n%s", out)
	}

	// Bad requests fail before anything is created.
	if _, errOut, err := jk(t, ns, "deploy", cliCharm(t, "jk-test.charm"), "--config", "nope=1"); err == nil || !strings.Contains(errOut, `unknown option "nope"`) {
		t.Fatalf("deploy with an unknown option: %v %q", err, errOut)
	}
	if _, errOut, err := jk(t, ns, "deploy", cliCharm(t, "jk-test.charm"), "--config", "greeting"); err == nil || !strings.Contains(errOut, "key=value") {
		t.Fatalf("deploy with a bad config: %v %q", err, errOut)
	}
	if out := jkOK(t, ns, "status"); !strings.Contains(out, "Model is empty.") {
		t.Fatalf("a rejected deploy left something behind:\n%s", out)
	}

	out := jkOK(t, ns, "deploy", cliCharm(t, "jk-test.charm"), "-n", "2", "--config", "greeting=hi")
	if !strings.Contains(out, `Deployed "jk-test" from local charm "jk-test"`) {
		t.Fatalf("deploy output: %s", out)
	}
	jkOK(t, ns, "deploy", cliCharm(t, "jk-test-client.charm"))
	if _, errOut, err := jk(t, ns, "deploy", cliCharm(t, "jk-test-client.charm")); err == nil || !strings.Contains(errOut, "already exists") {
		t.Fatalf("second deploy: %v %q", err, errOut)
	}
	if out := jkOK(t, ns, "integrate", "jk-test", "jk-test-client"); !strings.Contains(out, "Integrated jk-test:data with jk-test-client:source") {
		t.Fatalf("integrate: %s", out)
	}
	if _, errOut, err := jk(t, ns, "integrate", "jk-test", "jk-test-client"); err == nil || !strings.Contains(errOut, "already exists") {
		t.Fatalf("second integrate: %v %q", err, errOut)
	}
	if _, errOut, err := jk(t, ns, "integrate", "jk-test:nope", "jk-test-client"); err == nil || !strings.Contains(errOut, "no compatible endpoints") {
		t.Fatalf("bad endpoint: %v %q", err, errOut)
	}

	waitCLIActive(t, ns, "jk-test", 2, "greeting: hi")
	waitCLIActive(t, ns, "jk-test-client", 1, "providers: 2")

	text := jkOK(t, ns, "status", "--relations")
	// One of the two units leads (which one depends on which became ready first).
	if !strings.Contains(text, "jk-test/0*") && !strings.Contains(text, "jk-test/1*") {
		t.Errorf("no leader marked:\n%s", text)
	}
	for _, want := range []string{"jk-test-client/0*", "Integration provider", "jk-test:data", "jk-test-client:source", "jk_test_data", "regular", "jk_test_peers"} {
		if !strings.Contains(text, want) {
			t.Errorf("status lacks %q:\n%s", want, text)
		}
	}

	// Actions: the leader, a parameter with nested keys, an action that fails, a command.
	if out := jkOK(t, ns, "actions", "jk-test"); !strings.Contains(out, "echo") || !strings.Contains(out, "whoami") {
		t.Fatalf("actions:\n%s", out)
	}
	out = jkOK(t, ns, "run", "jk-test/leader", "whoami")
	if !strings.Contains(out, "Running operation 1 with 1 task") || !strings.Contains(out, "leader:") || !strings.Contains(out, "app: jk-test") {
		t.Fatalf("run:\n%s", out)
	}
	if _, errOut, err := jk(t, ns, "run", "jk-test/0", "fail"); err == nil || !strings.Contains(errOut, "failed on purpose") {
		t.Fatalf("run fail: %v %q", err, errOut)
	}
	if _, errOut, err := jk(t, ns, "run", "jk-test/0", "echo", "nope=1"); err == nil || !strings.Contains(errOut, "validation failed") {
		t.Fatalf("run with a bad parameter: %v %q", err, errOut)
	}
	if _, errOut, err := jk(t, ns, "run", "jk-test/0", "nope"); err == nil || !strings.Contains(errOut, `action "nope" not defined`) {
		t.Fatalf("run an unknown action: %v %q", err, errOut)
	}
	out = jkOK(t, ns, "exec", "--application", "jk-test", "--", "echo", "from-$JUJU_UNIT_NAME")
	if strings.Count(out, "from-jk-test/") != 2 || !strings.Contains(out, "jk-test/0:") {
		t.Fatalf("exec:\n%s", out)
	}
	if _, errOut, err := jk(t, ns, "exec", "--unit", "jk-test/0", "--", "exit 3"); err == nil || !strings.Contains(errOut, "exited with status 3") {
		t.Fatalf("exec failing: %v %q", err, errOut)
	}
	if out := jkOK(t, ns, "ssh", "jk-test/0", "echo", "hello"); strings.TrimSpace(out) != "hello" {
		t.Fatalf("ssh: %q", out)
	}
	if out := jkOK(t, ns, "debug-log", "--no-tail", "--include", "jk-test/0"); !strings.Contains(out, "unit-jk-test-0: ") || strings.Contains(out, "unit-jk-test-1: ") {
		t.Fatalf("debug-log:\n%s", out)
	}
	if out := jkOK(t, ns, "show-unit", "jk-test/1"); !strings.Contains(out, "pod: jk-test-1") {
		t.Fatalf("show-unit:\n%s", out)
	}
	bg := jkOK(t, ns, "run", "jk-test/0", "whoami", "--background")
	if !strings.Contains(bg, "show-operation") {
		t.Fatalf("background run:\n%s", bg)
	}
	ops := jkOK(t, ns, "operations")
	for _, want := range []string{"whoami run on jk-test/", "fail run on jk-test/0", "juju-exec run on jk-test/0,jk-test/1", "completed", "failed"} {
		if !strings.Contains(ops, want) {
			t.Errorf("operations lacks %q:\n%s", want, ops)
		}
	}
	if out := jkOK(t, ns, "show-task", "2", "--format", "json"); !strings.Contains(out, `"action": "whoami"`) {
		t.Fatalf("show-task:\n%s", out)
	}
	if out := jkOK(t, ns, "show-operation", "1"); !strings.Contains(out, "status: completed") {
		t.Fatalf("show-operation:\n%s", out)
	}

	// Config through the CLI reaches the charm, and bad values are refused.
	jkOK(t, ns, "config", "jk-test", "greeting=yo")
	waitCLIActive(t, ns, "jk-test", 2, "greeting: yo")
	if got := strings.TrimSpace(jkOK(t, ns, "config", "jk-test", "greeting")); got != "yo" {
		t.Fatalf("config get: %q", got)
	}
	if _, errOut, err := jk(t, ns, "config", "jk-test", "nope=1"); err == nil || !strings.Contains(errOut, "unknown option") {
		t.Fatalf("config unknown: %v %q", err, errOut)
	}
	jkOK(t, ns, "config", "jk-test", "--reset", "greeting")
	waitCLIActive(t, ns, "jk-test", 2, "greeting: hello") // the charm's default is "hello"
}

func TestCLIScaleRefreshRemove(t *testing.T) {
	t.Parallel()
	ns := cliModel(t)
	jkOK(t, ns, "deploy", cliCharm(t, "jk-test.charm"), "--trust", "--scope", "namespace")
	jkOK(t, ns, "deploy", cliCharm(t, "jk-test-client.charm"))
	jkOK(t, ns, "integrate", "jk-test", "jk-test-client")
	waitCLIActive(t, ns, "jk-test", 1, "clients: 1")

	// --trust with a scope maps to that trust level; the plain flag would be cluster.
	if a, err := applicationOf(ns, "jk-test"); err != nil || a.Spec.Trust != "namespace" {
		t.Fatalf("trust: %v %v", err, a)
	}
	jkOK(t, ns, "trust", "jk-test")
	if a, _ := applicationOf(ns, "jk-test"); a.Spec.Trust != "cluster" {
		t.Fatalf("trust after `trust`: %v", a.Spec.Trust)
	}
	jkOK(t, ns, "trust", "jk-test", "--remove")
	if a, _ := applicationOf(ns, "jk-test"); a.Spec.Trust != "none" {
		t.Fatalf("trust after --remove: %v", a.Spec.Trust)
	}

	// Scale up and down by count.
	jkOK(t, ns, "add-unit", "jk-test", "-n", "2")
	waitCLIActive(t, ns, "jk-test", 3, "peers: 2")
	jkOK(t, ns, "remove-unit", "jk-test", "--num-units", "2")
	waitCLIActive(t, ns, "jk-test", 1, "peers: 0")
	jkOK(t, ns, "scale-application", "jk-test", "2")
	waitCLIActive(t, ns, "jk-test", 2, "peers: 1")
	if _, errOut, err := jk(t, ns, "remove-unit", "jk-test/1"); err == nil || !strings.Contains(errOut, "by count") {
		t.Fatalf("remove-unit by name: %v %q", err, errOut)
	}

	// Refresh to a local revision 2.
	if out := jkOK(t, ns, "refresh", "jk-test", "--path", cliCharm(t, "jk-test-rev2.charm")); !strings.Contains(out, `Added local charm "jk-test"`) {
		t.Fatalf("refresh: %s", out)
	}
	waitCLIActive(t, ns, "jk-test", 2, "rev: 2")
	if _, errOut, err := jk(t, ns, "refresh", "jk-test"); err == nil || !strings.Contains(errOut, "refresh it with --path") {
		t.Fatalf("refresh without path: %v %q", err, errOut)
	}

	// Remove the relation: both sides see it go.
	jkOK(t, ns, "remove-relation", "jk-test", "jk-test-client")
	eventually(t, time.Minute, "the relation to be removed", func() (bool, error) {
		st := cliStatus(t, ns)
		for _, r := range st.Relations {
			if r.Type == "regular" {
				return false, fmt.Errorf("%+v", r)
			}
		}
		return true, nil
	})
	waitCLIActive(t, ns, "jk-test-client", 1, "providers: 0")
	if _, errOut, err := jk(t, ns, "remove-relation", "jk-test", "jk-test-client"); err == nil || !strings.Contains(errOut, "no relation found") {
		t.Fatalf("second remove-relation: %v %q", err, errOut)
	}

	// Remove an application and wait for it.
	jkOK(t, ns, "remove-application", "jk-test-client")
	if _, ok := cliStatus(t, ns).Applications["jk-test-client"]; ok {
		t.Fatal("application still listed after remove-application")
	}

	// Model config round trip.
	jkOK(t, ns, "model-config", "update-status-hook-interval=15s")
	if got := strings.TrimSpace(jkOK(t, ns, "model-config", "update-status-hook-interval")); got != "15s" {
		t.Fatalf("model-config: %q", got)
	}

	// A graceful destroy removes everything and the namespace.
	jkOK(t, ns, "destroy-model", ns, "--no-prompt")
	if err := kube.Get(context.Background(), client.ObjectKey{Name: ns}, &corev1.Namespace{}); !apierrors.IsNotFound(err) {
		t.Fatalf("namespace still there: %v", err)
	}
}

func TestCLIDestroyModelForce(t *testing.T) {
	t.Parallel()
	ns := cliModel(t)
	jkOK(t, ns, "deploy", cliCharm(t, "jk-test.charm"))
	waitCLIActive(t, ns, "jk-test", 1, "greeting")
	// Without confirmation (and no terminal) nothing happens.
	if _, errOut, err := jk(t, ns, "destroy-model", ns, "--force"); err == nil || !strings.Contains(errOut, "refusing to prompt") {
		t.Fatalf("destroy without confirmation: %v %q", err, errOut)
	}
	if err := kube.Get(context.Background(), client.ObjectKey{Name: ns}, &corev1.Namespace{}); err != nil {
		t.Fatal(err)
	}
	if _, errOut, err := jk(t, "kube-system", "destroy-model", "kube-system", "--force", "--no-prompt"); err == nil || !strings.Contains(errOut, "not a jk model") {
		t.Fatalf("destroying a namespace that is not a model: %v %q", err, errOut)
	}
	jkOK(t, ns, "destroy-model", ns, "--force", "--no-prompt", "--destroy-storage")
	if err := kube.Get(context.Background(), client.ObjectKey{Name: ns}, &corev1.Namespace{}); !apierrors.IsNotFound(err) {
		t.Fatalf("namespace still there: %v", err)
	}
	if out := jkOK(t, "", "models"); strings.Contains(out, ns) {
		t.Fatalf("models still lists it:\n%s", out)
	}
}

func TestCLIUnsupported(t *testing.T) {
	t.Parallel()
	if _, errOut, err := jk(t, "", "bootstrap"); err == nil || !strings.Contains(errOut, "kubectl jk install") {
		t.Fatalf("bootstrap: %v %q", err, errOut)
	}
}

// encodeBasic is the value of a basic-auth Authorization header.
func encodeBasic(user, password string) string {
	return base64.StdEncoding.EncodeToString([]byte(user + ":" + password))
}
