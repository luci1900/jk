package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

// unsupported maps juju commands jk does not have to what to do instead ("" when there is no alternative).
var unsupported = map[string]string{
	// Not planned: jk has no controllers, clouds, users, machines or backends.
	"bootstrap":            "use `kubectl jk install`",
	"destroy-controller":   "use `kubectl jk uninstall`",
	"kill-controller":      "use `kubectl jk uninstall`",
	"add-cloud":            "the kubeconfig already names the cluster",
	"add-credential":       "the kubeconfig already holds the credentials",
	"controllers":          "use `kubectl config get-contexts`",
	"clouds":               "use `kubectl config get-contexts`",
	"login":                "use `kubectl config use-context`",
	"logout":               "",
	"whoami":               "use `kubectl auth whoami`",
	"add-user":             "users come from the cluster's identity provider",
	"register":             "users come from the cluster's identity provider",
	"grant":                "use RBAC: `kubectl create rolebinding --clusterrole=jk-writer`",
	"revoke":               "use RBAC",
	"machines":             "",
	"add-machine":          "",
	"spaces":               "",
	"subnets":              "",
	"add-storage":          "",
	"attach-storage":       "",
	"add-secret-backend":   "secrets are k8s Secrets",
	"export-bundle":        "",
	"bundle":               "",
	"upgrade-model":        "not available yet",
	"show-controller":      "",
	"change-user-password": "",
	// Planned later.
	"debug-hooks":     "not available yet",
	"expose":          "use an ingress charm",
	"unexpose":        "use an ingress charm",
	"consume":         "relate with `integrate <application> <model>.<offer>`",
	"remove-saas":     "use `remove-relation`",
	"add-secret":      "not available yet",
	"update-secret":   "not available yet",
	"grant-secret":    "not available yet",
	"revoke-secret":   "not available yet",
	"remove-secret":   "not available yet",
	"secrets":         "not available yet",
	"show-secret":     "not available yet",
	"attach-resource": "not available yet",
	"resources":       "not available yet",
}

// unsupportedCommands registers the juju commands jk lacks, so they fail with a clear message instead of "unknown command".
func unsupportedCommands() []*cobra.Command {
	var out []*cobra.Command
	for name, alt := range unsupported {
		name, alt := name, alt
		out = append(out, &cobra.Command{
			Use:                name,
			Hidden:             true,
			DisableFlagParsing: true,
			RunE: func(*cobra.Command, []string) error {
				msg := fmt.Sprintf("%q is not supported in jk", name)
				if alt != "" {
					msg += ": " + alt
				}
				return fmt.Errorf("%s", msg)
			},
		})
	}
	return out
}
