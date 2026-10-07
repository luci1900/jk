//go:build e2e && postgres && cos

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
)

// The milestone scenario for offers: COS Lite (prometheus, loki and grafana) in its own model, offering its three
// endpoints; postgresql-k8s in another model related to all three through the CLI. Metrics are scraped, logs arrive and
// the dashboards are loaded. Run with `make test-e2e-cos`; the COS charms exist for amd64 only.
const (
	cosChannel = "2/stable"
	cosSettle  = 25 * time.Minute
)

func requireAmd64(t *testing.T) {
	t.Helper()
	var nodes corev1.NodeList
	if err := kube.List(context.Background(), &nodes); err != nil {
		t.Fatal(err)
	}
	for _, n := range nodes.Items {
		if a := n.Status.NodeInfo.Architecture; a != "amd64" {
			t.Skipf("the COS Lite charms are published for amd64; node %s is %s", n.Name, a)
		}
	}
}

// pythonGet runs an HTTP GET inside a unit's charm container (the workload shares the pod's network) and returns the body.
func pythonGet(t *testing.T, model, unit, url, auth string) (string, error) {
	t.Helper()
	header := ""
	if auth != "" {
		header = fmt.Sprintf(`, headers={"Authorization": "Basic %s"}`, auth)
	}
	script := fmt.Sprintf(`import urllib.request as u; print(u.urlopen(u.Request("%s"%s), timeout=20).read().decode())`, url, header)
	out, errOut, err := jk(t, model, "ssh", unit, "python3", "-c", script)
	if err != nil {
		return "", fmt.Errorf("%v: %s %s", err, out, errOut)
	}
	return out, nil
}

func TestCOSLite(t *testing.T) {
	requireAmd64(t)
	cos, db := cliModel(t), cliModel(t)
	begin := time.Now()
	lap := func(what string) { t.Logf("[%s] %s", time.Since(begin).Round(time.Second), what) }

	for _, c := range []string{"prometheus-k8s", "loki-k8s", "grafana-k8s"} {
		jkOK(t, cos, "deploy", c, "--channel", cosChannel, "--trust")
	}
	jkOK(t, cos, "integrate", "prometheus-k8s:grafana-source", "grafana-k8s:grafana-source")
	jkOK(t, cos, "integrate", "loki-k8s:grafana-source", "grafana-k8s:grafana-source")
	jkOK(t, db, "deploy", pg, "--channel", pgChannel, "--trust")

	eventually(t, cosSettle, "COS Lite and postgres active", func() (bool, error) {
		var waiting []string
		for ns, apps := range map[string][]string{cos: {"prometheus-k8s", "loki-k8s", "grafana-k8s"}, db: {pg}} {
			st := cliStatus(t, ns)
			for _, name := range apps {
				if a := st.Applications[name]; a.Status.Current != "active" {
					var units []string
					for u, us := range a.Units {
						units = append(units, fmt.Sprintf("%s workload=%s %q agent=%s", u, us.WorkloadStatus.Current, us.WorkloadStatus.Message, us.AgentStatus.Current))
					}
					sort.Strings(units)
					waiting = append(waiting, fmt.Sprintf("%s: %s %q %v", name, a.Status.Current, a.Status.Message, units))
				}
			}
		}
		if len(waiting) > 0 {
			return false, fmt.Errorf("%s", strings.Join(waiting, "; "))
		}
		return true, nil
	})
	lap("COS Lite and postgres are up")

	// COS offers what a monitored model needs, to every model (as the COS Lite overlay does).
	jkOK(t, cos, "offer", "prometheus-k8s:metrics-endpoint", "prometheus-scrape", "--allow", "*")
	jkOK(t, cos, "offer", "loki-k8s:logging", "loki-logging", "--allow", "*")
	jkOK(t, cos, "offer", "grafana-k8s:grafana-dashboard", "grafana-dashboards", "--allow", "*")
	for _, rel := range [][2]string{
		{pg + ":metrics-endpoint", cos + ".prometheus-scrape"},
		{pg + ":logging", cos + ".loki-logging"},
		{pg + ":grafana-dashboard", cos + ".grafana-dashboards"},
	} {
		if out := jkOK(t, db, "integrate", rel[0], rel[1]); !strings.Contains(out, "Integrated") {
			t.Fatalf("integrate %v: %s", rel, out)
		}
	}
	lap("postgres is related to the three offers")
	if out := jkOK(t, db, "status", "--relations"); !strings.Contains(out, "SAAS") || !strings.Contains(out, "prometheus-scrape") {
		t.Fatalf("status:\n%s", out)
	}

	// Prometheus scrapes postgres: a target of the application, up.
	eventually(t, cosSettle, "prometheus to scrape postgresql-k8s", func() (bool, error) {
		body, err := pythonGet(t, cos, "prometheus-k8s/0", "http://localhost:9090/api/v1/targets", "")
		if err != nil {
			return false, err
		}
		var resp struct {
			Data struct {
				ActiveTargets []struct {
					Labels map[string]string `json:"labels"`
					Health string            `json:"health"`
				} `json:"activeTargets"`
			} `json:"data"`
		}
		if err := json.Unmarshal([]byte(body), &resp); err != nil {
			return false, err
		}
		var seen []string
		for _, tg := range resp.Data.ActiveTargets {
			seen = append(seen, tg.Labels["juju_application"]+"="+tg.Health)
			if tg.Labels["juju_application"] == pg && tg.Health == "up" {
				return true, nil
			}
		}
		return false, fmt.Errorf("targets %v", seen)
	})
	lap("prometheus scrapes postgres")

	// Loki receives postgres's logs.
	eventually(t, cosSettle, "loki to receive logs of postgresql-k8s", func() (bool, error) {
		body, err := pythonGet(t, cos, "loki-k8s/0", "http://localhost:3100/loki/api/v1/label/juju_application/values", "")
		if err != nil {
			return false, err
		}
		if !strings.Contains(body, `"`+pg+`"`) {
			return false, fmt.Errorf("label values %s", strings.TrimSpace(body))
		}
		return true, nil
	})
	lap("loki has postgres's logs")

	// Grafana loads postgres's dashboards.
	out := jkOK(t, cos, "run", "grafana-k8s/leader", "get-admin-password", "--format", "json")
	var tasks map[string]map[string]any
	if err := json.Unmarshal([]byte(out), &tasks); err != nil {
		t.Fatal(err)
	}
	password := ""
	for _, task := range tasks {
		if res, ok := task["results"].(map[string]any); ok {
			password, _ = res["admin-password"].(string)
		}
	}
	if password == "" {
		t.Fatalf("no admin password in %s", out)
	}
	eventually(t, cosSettle, "grafana to load postgres's dashboards", func() (bool, error) {
		auth := encodeBasic("admin", password)
		body, err := pythonGet(t, cos, "grafana-k8s/0", "http://localhost:3000/api/search?query=PostgreSQL", auth)
		if err != nil {
			return false, err
		}
		var found []map[string]any
		if err := json.Unmarshal([]byte(body), &found); err != nil {
			return false, err
		}
		if len(found) == 0 {
			return false, fmt.Errorf("no dashboard yet")
		}
		return true, nil
	})
	lap("grafana has postgres's dashboards")

	// Taking the monitoring away leaves postgres working and the COS side clean.
	for _, alias := range []string{"prometheus-scrape", "loki-logging", "grafana-dashboards"} {
		jkOK(t, db, "remove-relation", pg, alias)
	}
	eventually(t, 5*time.Minute, "no connections left on the offers", func() (bool, error) {
		out := jkOK(t, cos, "offers", "--format", "json")
		if strings.Contains(out, `"relation"`) {
			return false, fmt.Errorf("%s", out)
		}
		return true, nil
	})
	waitPGHealthy(t, pgRecover, db, 1)
}
