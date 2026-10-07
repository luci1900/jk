# Layout spike: notes

Result: postgresql-k8s 14/stable (rev 959, arm64, ubuntu@22.04) `install` runs to exit 0 from a hand-written StatefulSet on kind-jk-dev; `start` also exits 0. Trace: `trace-install.txt`.

## What is here

- `statefulset.yaml`: ServiceAccount, Role/RoleBinding (spike-only, wide), headless Service `postgresql-k8s-endpoints`, Service `postgresql-k8s`, StatefulSet with init container, `charm` container and `postgresql` workload container.
- `Dockerfile`: init-container image (`FROM scratch` with `jk-agent` and `pebble`), loaded into kind as `kind.local/jk-spike-agent:dev`.
- `push/`: turns the `.charm` zip into a single-layer linux/arm64 image and pushes it to `jk-registry` through a port-forward.
- `run.sh`: builds, loads, pushes and applies into namespace `jk-spike`. Needs `PEBBLE` (linux/arm64 pebble binary) and `CHARM` (the `.charm`).
- Code: `cmd/jk-agent` (`init` and `unit` subcommands, multicall hook tools) and `internal/hooktools` (unix-socket protocol, thin client, canned in-memory handler).

## Layout that worked

- Pod `postgresql-k8s-0`, one `charm-data` emptyDir mounted with subPaths, as juju: `/charm/bin` (ro in `charm`), `/var/lib/juju`, `/charm/containers`, `/var/lib/pebble/default` (= `containeragent/pebble`), `/var/log/juju`.
- Init container `charm-init` (image: jk-agent + pebble, runs as root): copies `/jk-agent` and `/pebble` to `/charm/bin`, writes the Pebble layer `001-container-agent.yaml` into `/containeragent/pebble/layers`, creates `/charm/containers/<name>`, pulls the charm image from `jk-registry.jk-system.svc:5000/charms/postgresql-k8s@sha256:...` over plain HTTP and extracts it to `/var/lib/juju/agents/unit-postgresql-k8s-0/charm`.
- Container `charm`: image `ghcr.io/juju/charm-base:ubuntu-22.04`, command `/charm/bin/pebble run --http :38812 --verbose` (Pebble is PID 1, as in juju 3.6), workingDir `/var/lib/juju`, env `JUJU_CONTAINER_NAMES`, `HTTP_PROBE_PORT`; Pebble starts the agent from the layer: `/charm/bin/jk-agent unit --data-dir /var/lib/juju`. Probes on Pebble's `/v1/health` (startup/liveness/readiness) like juju.
- Container `postgresql`: the charm's workload image, command `/charm/bin/pebble run --create-dirs --hold --http :38813 --verbose`, env `JUJU_CONTAINER_NAME`, `PEBBLE_SOCKET=/charm/container/pebble.socket`, `PEBBLE=/charm/container/pebble`, `PEBBLE_COPY_ONCE=/var/lib/pebble/default`, mounts `/charm/bin/pebble` (subPath, ro) and `/charm/container` (= `charm/containers/postgresql`). The socket shows up at `/charm/containers/postgresql/pebble.socket` in `charm`, and `pebble version` from `charm` reaches the workload's Pebble.
- Storage: `volumeClaimTemplates` `postgresql-k8s-pgdata`, mounted at `/var/lib/postgresql/data` in the workload and `/var/lib/juju/storage/pgdata/0` in `charm`.
- All containers run as root (juju's pre-3.5 behaviour, which applies when the charm declares no rootless user).
- Workload image: the real one. `metadata.yaml` has `upstream-source: ghcr.io/canonical/charmed-postgresql@sha256:11db80a0...` which is public and multi-arch; Charmhub's resource API instead returns `registry.jujucharms.com/...` plus a token (what docs/design.md (Charm delivery) describes).
- Hook run: `cd <charm dir> && dispatch` with `JUJU_DISPATCH_PATH=hooks/install`, environment replaced (not inherited) with juju's HookVars, `PATH=<tools dir>:$PATH`, tools dir `/var/lib/juju/tools/unit-postgresql-k8s-0/` holding one symlink per tool to `/charm/bin/jk-agent`.

## Deviations from juju and why

- Init container image is a custom `scratch` image with `/jk-agent` and `/pebble`; juju uses the jujud-operator image (`/opt/containeragent`, `/opt/jujuc`, `/opt/pebble`). jk has one multicall binary, so no separate `containeragent`/`jujuc`.
- The init container fetches the charm itself and writes it where juju's unit agent would unpack it; there is no controller introduction, `agent.conf` or `template-agent.conf`.
- Unit identity and model come from env (`JK_POD_NAME` from the downward API, `JK_MODEL_NAME`, `JK_MODEL_UUID`) instead of `agent.conf`. The namespace UID is not available through the downward API, so the operator has to inject it as an env value (or the agent reads the Namespace; needs RBAC).
- Not mounted: `/etc/profile.d/juju-introspection.sh`, `/usr/bin/juju-exec`, `juju-introspect`, Pebble identities (no rootless charm), `imagePullSecrets` (workload image is public), `CAAS_IMAGE_REPO` secrets. `/var/log/juju` is mounted but unused.
- Labels and annotations are reduced to `app.kubernetes.io/name` and `managed-by`; juju's full set and ownerReferences are not reproduced. Volume claim name is simplified (juju uses `<app>-<storage>-<hash>`).
- No `fsGroup`/`supplementalGroups` (only set for rootless charms).
- Hook tool server is JSON over a unix socket (`agent.socket`), not juju's net/rpc `jujuc` protocol. Charms only see the CLI, so this is invisible to them.
- The agent runs a single hook and then idles; no resolver, leadership, or flush-on-success.
- Role/RoleBinding for the app is `*` on everything in the namespace (spike only; juju grants a narrower Role, plus a ClusterRole for `trust`).

## Hook environment ops required

- ops fails without: `JUJU_DISPATCH_PATH`, `JUJU_HOOK_NAME`, `JUJU_MODEL_NAME`, `JUJU_MODEL_UUID`, `JUJU_UNIT_NAME`, `JUJU_VERSION` (ops `JujuContext.from_environ`), plus `JUJU_CHARM_DIR`/`CHARM_DIR`, `JUJU_CONTEXT_ID` and `JUJU_AGENT_SOCKET_ADDRESS`/`_NETWORK` for the tools.
- Also set (as juju's `HookVars`): `JUJU_API_ADDRESSES`, `JUJU_MACHINE_ID`, `JUJU_PRINCIPAL_UNIT`, `JUJU_AVAILABILITY_ZONE`, `CLOUD_API_VERSION`, `JUJU_CHARM_{HTTP,HTTPS,FTP,NO}_PROXY`, `JUJU_CHARM_TRACE_CONFIG_{HTTP,GRPC,CA_CERT}`, the Ubuntu vars (`LANG`, `DEBIAN_FRONTEND`, ...), and the allow-listed `KUBERNETES_*` vars (needed by the charm's lightkube client).
- `JUJU_CONTAINER_NAMES` is not in the hook environment in juju (only in the pod env) and ops/this charm did not need it. `PEBBLE_SOCKET` is not set for hooks either: ops defaults to `/charm/containers/<name>/pebble.socket`.
- `JUJU_VERSION` was `3.6.9`; the charm's `assumes` (`juju >= 3.5.1, < 4`) is not checked by ops.
- Charm dir must be writable: `dispatch` creates `venv/bin/python` (symlink to `python3`) and ops may write `.unit-state.db`. `python3` (3.10) must exist in the charm container, so the charm-base image is required.

## Hook tools

- Called by `install` (ops 3.8.2, `use_juju_for_storage=True`): `juju-log`, `status-get`, `state-get`, `state-set`, `state-delete`, `relation-ids`, `relation-list`, `relation-get`, `secret-get` (24 calls, all "not found"; no `secret-add` happened in install or start), `opened-ports`, `open-port`, `is-leader`. `start` adds nothing new. Neither hook calls `status-set` (the charm sets status in other events).
- Formats that mattered: `state-get` of a missing key prints nothing with exit 0 (juju's default "smart" format); `state-set --file -` and `relation-set --file -` read YAML on stdin (ops sends YAML for state, JSON for relation-set); `relation-get` of an unset peer returns `{}` not `null` and a unit's own settings are pre-seeded with `egress-subnets`, `ingress-address`, `private-address`; peer relations exist (`database-peers:0`) before any `relation-created`; `secret-get` on a missing label must print `... not found` on stderr (ops maps that to `SecretNotFoundError`); `secret-add` takes `key#file=<path>` and the file is read by the agent, so the tool and agent must share a filesystem (they do: same container); `status-get --application=false` is `--flag=value` form; flags can follow positionals (`relation-get -r X - unit`).
- Implemented canned (needed or plausible next): all of juju 4's set (`internal/hooktools.Names`) is symlinked; `config-get` (defaults from `config.yaml`), `network-get`, `unit-get`, `goal-state`, `storage-list`/`storage-get`, `secret-*` in memory, `status-set`, `relation-set`. Returning an error: `credential-get`, `resource-get`, `storage-add`, `action-*` outside actions.

## Problems hit

- `JUJU_CONTAINER_NAMES` needs to be in the init container env too, to create `/charm/containers/<name>` (juju derives it from the same env).
- First state-get implementation returned JSON-quoted/errored for missing keys and crashed ops' storage (`StoredStateData[_stored]`); `relation-get` returning `null` crashed the charm (`TypeError`); `secret-get` was unimplemented. All three are fixed in `internal/hooktools/canned*.go`.
- `go install github.com/canonical/pebble/cmd/pebble@v1.32.1` with `GOOS=linux GOARCH=arm64` gives a static binary (juju 4 pins v1.32.1 in go.mod). `ko` cannot add it to an image.
- A 14/stable `.charm` is ~29 MB and contains the venv; extracting is instant, the first pull of `charmed-postgresql` is large.
- kind node arch is arm64 (Apple silicon); everything here is `linux/arm64`.
- `jk-registry` currently accepts anonymous pushes (zot config has no auth), unlike docs/design.md (Charm delivery).

## Suggested plan edits (not applied)

- Section 5, charm container: say Pebble is PID 1 of the `charm` container and the agent is a Pebble service (layer written by init), with probes on Pebble's `/v1/health`; as in juju 3.6. Port `38812` for `charm`, `38813+i` for workloads.
- Section 5, init container: say it runs `jk-agent init` from an agent image that also contains `pebble` (pin the version; juju 4 uses v1.32.1), and list what it writes: `/charm/bin/{jk-agent,pebble}`, the Pebble layer, `/charm/containers/<name>`, the charm at `/var/lib/juju/agents/unit-<app>-<n>/charm`. Note hook tools are symlinks created by the agent at start in `/var/lib/juju/tools/unit-<app>-<n>/`.
- Section 5/6: unit identity (pod name, model name, model UUID = namespace UID) reaches the agent as env set by the operator, because the downward API cannot give the namespace UID.
- Section 6, hook execution: the hook environment is built from scratch, not inherited; list the required `JUJU_*` set above and that the charm dir must be writable and the `charm` container must have `python3` (charm-base).
- Section 6, hook tools: add the wire facts (`--flag=value`, flags after positionals, YAML stdin for `state-set`/`relation-set`, `secret-add key#file=`) and that missing-key `state-get` is empty/exit 0. Peer relations and `database-peers:N` ids must exist from the first hook.
- Section 11: the charm image should be a single-layer linux/<arch> image with the charm contents at the root, pushed by digest; the init container pulls it with plain HTTP in-cluster and extracts it itself (no containerd involved), so the registry needs no node configuration, as planned. Charm-base and workload images are still pulled by nodes from ghcr.io; `upstream-source` in `metadata.yaml` is directly pullable for public charms, Charmhub's `registry.jujucharms.com` path plus token is only needed for private resources.
- Section 11, access: `jk-registry` config must add auth (anonymous pull, authenticated push); currently open.
- `status-set` is not exercised by postgresql-k8s install; track hook tools per charm event, not per install.

- Update: `jk-registry` now requires auth to push (user `jk`, password in Secret `jk-system/jk-registry-auth`). `push/main.go` and `run.sh` predate this and push anonymously, so they need credentials added before re-running.
