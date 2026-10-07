//go:build e2e && postgres

package e2e

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

// The data-integrator scenario: postgresql-k8s 14/stable and data-integrator latest/stable, related by creating a Relation,
// then the integrator's get-credentials action returns the database's endpoints, username and password.
// One postgres unit keeps it quick; run with `make test-e2e-postgres`.
const (
	integrator        = "data-integrator"
	integratorChannel = "latest/stable"
	integratorDB      = "jkdb"
	// The integrator requires the endpoint "postgresql"; postgresql-k8s provides "database" (interface postgresql_client).
	pgProviderEP = "database"
	diRequirerEP = "postgresql"
)

// findCredentials returns the first map in the results (searched depth first) that holds username, password and endpoints.
func findCredentials(v any) map[string]any {
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	if _, u := m["username"]; u {
		if _, p := m["password"]; p {
			if _, e := m["endpoints"]; e {
				return m
			}
		}
	}
	for _, child := range m {
		if c := findCredentials(child); c != nil {
			return c
		}
	}
	return nil
}

func TestPostgresDataIntegrator(t *testing.T) {
	ns := cliModel(t)
	dataIntegratorScenario(t, ns, ns)
}

// The same, with postgresql-k8s offered by its model and the integrator in another one.
func TestPostgresDataIntegratorAcrossModels(t *testing.T) {
	dataIntegratorScenario(t, cliModel(t), cliModel(t))
}

// dataIntegratorScenario deploys postgresql-k8s in pgModel and data-integrator in diModel (the same model, or two)
// through the CLI, relates them (through an offer when the models differ), and reads the credentials.
func dataIntegratorScenario(t *testing.T, pgModel, diModel string) {
	cross := pgModel != diModel
	begin := time.Now()
	lap := func(what string) { t.Logf("[%s] %s", time.Since(begin).Round(time.Second), what) }

	// Everything through the CLI, as a user would type it.
	jkOK(t, pgModel, "deploy", pg, "--channel", pgChannel, "--trust")
	jkOK(t, diModel, "deploy", integrator, "--channel", integratorChannel, "--config", "database-name="+integratorDB)
	waitPGHealthy(t, pgFirstReady, pgModel, 1)
	lap("postgres is up")
	eventually(t, 5*time.Minute, "data-integrator resolved and its unit running", func() (bool, error) {
		a, ok := cliStatus(t, diModel).Applications[integrator]
		if !ok || a.CharmRev == 0 || len(a.Units) == 0 {
			return false, fmt.Errorf("%+v", a)
		}
		return podReady(diModel, unitOf(integrator, 0))
	})
	lap("data-integrator is running")

	remote := pg
	if cross {
		jkOK(t, pgModel, "offer", pg+":"+pgProviderEP, "--allow", diModel)
		remote = pgModel + "." + pg
	}
	out := jkOK(t, diModel, "integrate", integrator+":"+diRequirerEP, remote)
	if !strings.Contains(out, "Integrated") {
		t.Fatalf("integrate: %s", out)
	}
	if out := jkOK(t, diModel, "status", "--relations"); !strings.Contains(out, "postgresql_client") {
		t.Fatalf("status lacks the relation:\n%s", out)
	}

	// Both applications settle active after the integration (the integrator is blocked or waiting until the database exists).
	eventually(t, pgRecover, "postgres and data-integrator active after the integration", func() (bool, error) {
		for ns, name := range map[string]string{pgModel: pg, diModel: integrator} {
			if a := cliStatus(t, ns).Applications[name]; a.Status.Current != "active" {
				return false, fmt.Errorf("%s: %+v", name, a.Status)
			}
		}
		return true, nil
	})
	lap("both active after the integration")

	// The credentials can lag the active status by a hook or two, so ask until the results have them.
	var creds map[string]any
	eventually(t, 5*time.Minute, "get-credentials to return the database credentials", func() (bool, error) {
		out, errOut, err := jk(t, diModel, "run", integrator+"/leader", "get-credentials", "--format", "json")
		if err != nil {
			return false, fmt.Errorf("%v: %s", err, errOut)
		}
		var tasks map[string]map[string]any
		if err := json.Unmarshal([]byte(out), &tasks); err != nil {
			return false, err
		}
		for _, task := range tasks {
			if creds = findCredentials(task["results"]); creds == nil {
				return false, fmt.Errorf("no credentials in the results (top-level keys %v)", keys(task))
			}
		}
		return len(tasks) == 1, nil
	})
	lap("get-credentials returned")

	// Never log the password; only check it is there.
	str := func(k string) string { s, _ := creds[k].(string); return s }
	if str("username") == "" {
		t.Errorf("empty username in %v", keys(creds))
	}
	if str("password") == "" {
		t.Errorf("empty password")
	}
	// Across models the endpoint still names the database's own namespace.
	if want := "." + pgModel + ".svc"; !strings.Contains(str("endpoints"), ":") || !strings.Contains(str("endpoints"), want) {
		t.Errorf("endpoints %q, want host:port in %s", str("endpoints"), want)
	}
	if db := str("database"); db != "" && db != integratorDB {
		t.Errorf("database %q, want %q", db, integratorDB)
	}
	t.Logf("credentials: user %s, endpoints %s, database %s (password %d characters)", str("username"), str("endpoints"), str("database"), len(str("password")))

	// Removing the relation and the applications through the CLI leaves a clean model.
	jkOK(t, diModel, "remove-relation", integrator, pg) // the offer's name is the remote application's name here
	jkOK(t, diModel, "remove-application", integrator)
	if cross {
		jkOK(t, pgModel, "remove-offer", pg, "--force")
		jkOK(t, diModel, "destroy-model", diModel, "--no-prompt", "--force")
	}
	jkOK(t, pgModel, "destroy-model", pgModel, "--no-prompt", "--force")
}
