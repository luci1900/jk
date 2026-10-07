package hooktools

// Names lists juju 4's hook tools (juju 3.6 minus leader-get, leader-set, payloads and podspec tools).
// storage-add is not supported on k8s (as in juju) but is still listed here so that charms get a clear error rather than "command not found".
var Names = []string{
	"action-fail", "action-get", "action-log", "action-set",
	"application-version-set", "close-port", "config-get", "credential-get",
	"goal-state", "is-leader", "juju-log", "juju-reboot", "network-get",
	"open-port", "opened-ports", "relation-get", "relation-ids", "relation-list",
	"relation-model-get", "relation-set", "resource-get",
	"secret-add", "secret-get", "secret-grant", "secret-ids", "secret-info-get",
	"secret-remove", "secret-revoke", "secret-set",
	"state-delete", "state-get", "state-set", "status-get", "status-set",
	"storage-add", "storage-get", "storage-list", "unit-get",
}
