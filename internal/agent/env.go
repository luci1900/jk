// SPDX-License-Identifier: AGPL-3.0-only
//
// Derived from juju (https://github.com/juju/juju), AGPL-3.0: internal/worker/uniter/runner/context/env.go
// (ContextAllowedEnvVars and HookVars).

package agent

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/internal/agent/resolver"
)

// k8sEnv is the allow-list of pod environment passed to hooks (juju's ContextAllowedEnvVars).
var k8sEnv = []string{"KUBERNETES_PORT", "KUBERNETES_PORT_443_TCP", "KUBERNETES_PORT_443_TCP_ADDR", "KUBERNETES_PORT_443_TCP_PORT",
	"KUBERNETES_PORT_443_TCP_PROTO", "KUBERNETES_SERVICE", "KUBERNETES_SERVICE_HOST", "KUBERNETES_SERVICE_PORT", "KUBERNETES_SERVICE_PORT_HTTPS"}

const defaultPath = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

// modelProxy maps jk-model config keys (after the "model-config." prefix) to hook environment variables.
var modelProxy = []struct{ key, env string }{
	{"juju-http-proxy", "JUJU_CHARM_HTTP_PROXY"},
	{"juju-https-proxy", "JUJU_CHARM_HTTPS_PROXY"},
	{"juju-ftp-proxy", "JUJU_CHARM_FTP_PROXY"},
	{"juju-no-proxy", "JUJU_CHARM_NO_PROXY"},
	{"juju-charm-trace-config-http", "JUJU_CHARM_TRACE_CONFIG_HTTP"},
	{"juju-charm-trace-config-grpc", "JUJU_CHARM_TRACE_CONFIG_GRPC"},
	{"juju-charm-trace-config-ca-cert", "JUJU_CHARM_TRACE_CONFIG_CA_CERT"},
}

// modelValue looks up a model config key in the jk-model ConfigMap data.
func modelValue(data map[string]string, key string) string {
	if v, ok := data[ModelConfigPrefix+key]; ok {
		return v
	}
	return data[key]
}

// hookEnv builds the hook's environment from scratch (nothing is inherited from the pod except the allow-listed
// KUBERNETES_* variables and PATH): juju's HookVars, the Ubuntu variables and the dispatch path.
//
// An action (act is set) has no hook name, and JUJU_ACTION_NAME, JUJU_ACTION_UUID (the task id) and JUJU_ACTION_TAG
// are set; its dispatch path is actions/<name> (none for juju-exec, which runs a command).
func (a *Agent) hookEnv(info resolver.HookInfo, act *actionRun, ctxID, socket string, model map[string]string) []string {
	cfg := a.cfg
	name := info.Name()
	dispatch := "hooks/" + name
	if act != nil {
		name, dispatch = "", "actions/"+act.name
		if act.name == v1alpha1.ActionExec {
			dispatch = ""
		}
	}
	env := []string{}
	for _, k := range k8sEnv {
		if v, ok := cfg.Getenv(k); ok {
			env = append(env, k+"="+v)
		}
	}
	env = append(env,
		"CHARM_DIR="+cfg.CharmDir(),
		"JUJU_CHARM_DIR="+cfg.CharmDir(),
		"JUJU_CONTEXT_ID="+ctxID,
		"JUJU_HOOK_NAME="+name,
		"JUJU_AGENT_SOCKET_ADDRESS="+socket,
		"JUJU_AGENT_SOCKET_NETWORK=unix",
		"JUJU_UNIT_NAME="+cfg.Unit,
		"JUJU_MODEL_UUID="+cfg.ModelUUID,
		"JUJU_MODEL_NAME="+cfg.ModelName,
		"JUJU_API_ADDRESSES=",
		"JUJU_MACHINE_ID=",
		"JUJU_PRINCIPAL_UNIT=",
		"JUJU_AVAILABILITY_ZONE=",
		"JUJU_VERSION="+a.jujuVersion,
		"CLOUD_API_VERSION=",
		"JUJU_DISPATCH_PATH="+dispatch,
		"APT_LISTCHANGES_FRONTEND=none", "DEBIAN_FRONTEND=noninteractive", "LANG=C.UTF-8", "TERM=tmux-256color",
	)
	if act != nil {
		env = append(env, "JUJU_ACTION_NAME="+act.name, "JUJU_ACTION_UUID="+strconv.FormatInt(act.id, 10),
			"JUJU_ACTION_TAG=action-"+strconv.FormatInt(act.id, 10))
	}
	if info.Kind == resolver.PebbleReady {
		env = append(env, "JUJU_WORKLOAD_NAME="+info.WorkloadName)
	}
	if info.IsRelation() {
		env = append(env,
			"JUJU_RELATION="+info.Endpoint,
			fmt.Sprintf("JUJU_RELATION_ID=%s:%d", info.Endpoint, info.RelationID),
			"JUJU_REMOTE_UNIT="+info.RemoteUnit,
			"JUJU_REMOTE_APP="+info.RemoteApp)
		if info.DepartingUnit != "" {
			env = append(env, "JUJU_DEPARTING_UNIT="+info.DepartingUnit)
		}
	}
	if info.IsSecret() {
		env = append(env, "JUJU_SECRET_ID="+info.SecretURI, "JUJU_SECRET_LABEL="+info.SecretLabel)
		if info.Kind != resolver.SecretChanged || info.SecretRevision > 0 {
			if info.SecretRevision > 0 {
				env = append(env, "JUJU_SECRET_REVISION="+strconv.Itoa(info.SecretRevision))
			}
		}
	}
	if info.IsStorage() {
		name := resolver.StorageName(info.StorageID)
		env = append(env, "JUJU_STORAGE_ID="+info.StorageID, "JUJU_STORAGE_LOCATION="+a.storageDir(name), "JUJU_STORAGE_KIND=filesystem")
	}
	for _, p := range modelProxy {
		env = append(env, p.env+"="+modelValue(model, p.key))
	}
	path := defaultPath
	if v, ok := cfg.Getenv("PATH"); ok && v != "" {
		path = v
	}
	env = append(env, "PATH="+cfg.ToolsDir()+string(filepath.ListSeparator)+path)
	return env
}

func osGetenv(k string) (string, bool) { return os.LookupEnv(k) }

// firstIPv4 is the pod IP as seen from inside the pod.
func firstIPv4() string {
	addrs, _ := net.InterfaceAddrs()
	for _, a := range addrs {
		if n, ok := a.(*net.IPNet); ok && !n.IP.IsLoopback() && n.IP.To4() != nil {
			return n.IP.String()
		}
	}
	return ""
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
