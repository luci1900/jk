# jk

jk runs juju sidecar charms on Kubernetes with the k8s API as its only control plane. There is no controller. Charms run as they would under juju, and you drive them with `kubectl jk`, a CLI that follows juju's commands.

jk runs as a kubectl plugin, `kubectl jk <command>`. It uses your kubeconfig and the current kube context. A model is a namespace. `-m <model>` picks one, and the default is the context's namespace. `--help` on any command lists its flags.

To type `jk` instead of `kubectl jk`, add an alias to your shell:

```
alias jk='kubectl jk'
```

Tab completion works once kubectl's own completion is set up. For the alias in bash, also add `complete -o default -F __start_kubectl jk`.

## Install

Download the archive for your OS and CPU from the latest release and put `kubectl-jk` and `kubectl_complete-jk` on your PATH

Alternatively, build them with `make install` (needs Go), which puts them in your Go bin. Then install jk in the cluster:

```
jk install
```

This installs the CRDs, the operator and the registry in `jk-system` from the release images on ghcr. Run it again to upgrade. `--image-repo` changes where the images come from.

On MicroK8s, run `microk8s config > ~/.kube/config` first.

`jk uninstall` destroys every model first, so the teardown hooks run, then removes jk. It asks first, `-y` skips that, `--destroy-storage` deletes the volumes (they are kept by default) and `--force` skips the graceful removal.

## Models

```
jk add-model demo
jk models
jk switch demo
jk model-config
jk destroy-model demo
```

`destroy-model` waits for the teardown hooks. `--force` deletes the namespace at once. Volumes are kept unless you pass `--destroy-storage`.

## Applications

```
jk deploy postgresql-k8s --channel 14/stable --trust -n 3
jk deploy ./my.charm
jk config postgresql-k8s profile=testing
jk scale-application postgresql-k8s 1
jk refresh postgresql-k8s --channel 14/edge
jk remove-application postgresql-k8s
```

`--trust` gives the charm cluster access, and `--scope=namespace` narrows it. `jk find` and `info` search Charmhub.

## Relations and offers

```
jk integrate data-integrator postgresql-k8s
jk offer postgresql-k8s:database -m db
jk integrate data-integrator db.postgresql-k8s --alias pg
```

An offer lets other models relate to an application. `offer --allow <model>,...` says which models may use it (`*` for all). The default comes from the model config `offer-allowed-models`.

## Day to day

```
jk status --relations --watch
jk run data-integrator/leader get-credentials
jk exec --unit postgresql-k8s/0 -- ls /
jk resolved postgresql-k8s/0
jk show-unit postgresql-k8s/0
jk debug-log --include postgresql-k8s/0
jk ssh postgresql-k8s/0
jk set-constraints postgresql-k8s mem=4G
```

`resolved` retries a failed hook now, or skips it with `--no-retry`. `ssh` and `scp` use `kubectl exec` and `kubectl cp` on the charm container (`--container` picks another), so they need `kubectl` on the PATH and `tar` in the container. With a terminal, `ssh` into the charm container passes your `TERM` and opens a login bash, and other containers get `/bin/sh`. `debug-log` follows the charm containers of the model's pods. `set-constraints` replaces the old constraints, as in juju. Put `-m` before the unit when you use `ssh`.

## As YAML

Everything is a normal k8s object, so kubectl and GitOps work. Only the charm name is required.

```yaml
apiVersion: jk.luci1900.github.io/v1alpha1
kind: Application
metadata: {name: postgresql-k8s, namespace: db}
spec:
  charm: {name: postgresql-k8s, channel: 14/stable}
  scale: 3
  config: {profile: production}
  trust: namespace             # none | namespace | cluster
  storage:
    pgdata: {size: 10Gi}
---
kind: Relation
spec:
  endpoints:
    - {namespace: db, application: postgresql-k8s, endpoint: database}
    - {namespace: db, application: data-integrator, endpoint: postgresql}
```

## Access

Users are k8s users. `install` creates the ClusterRoles `jk-reader`, `jk-writer` and `jk-admin`. Grant access with a RoleBinding:

```
kubectl create rolebinding alex-write -n demo --clusterrole=jk-writer --user=alex
```

## Development

To run your own build, use one of these. They need Go and Docker, and they build, push or load the images and install jk:

```
make install-kind
make install-microk8s
```

`install-kind` creates a kind cluster named `jk-dev` if there is none, and loads the images into it. `install-microk8s` pushes to MicroK8s's registry addon, enabling it if needed, and uses the `microk8s` kube context. Rerun either after a change, as both restart the operator. `make clean-cluster` deletes the kind cluster.

`make install` only builds the CLI, so its `jk install` pulls the release images from ghcr. Use the targets above for a cluster that runs your build.

`make charm` builds the test charms in `bin/`. They need python3 with pip and network access. Try one with `jk deploy ./bin/jk-test.charm`.

- `make test` and `make test-envtest` are quick.
- `make install-kind` and then `make test-e2e` (about 2.5 minutes) is the end-to-end check to run locally.
- The postgresql-k8s and COS Lite scenarios (`make test-e2e-postgres`, `make test-e2e-cos`) are slow and run in CI. COS needs amd64.
- Run `make generate` after changing `api/`. CI checks that the output is committed.

Releases are made by pushing a `v*` tag, which publishes the images and the CLI archives.

