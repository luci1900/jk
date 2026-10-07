# jk

jk runs juju sidecar charms on Kubernetes with the k8s API as its only control plane. There is no controller. Charms run as they would under juju and you can interact with them using `kubectl jk`.

This is a hobby project, not something to run in production.

## Install

You need a cluster running Kubernetes 1.36 or newer. Either of these works:

- **kind:** [install kind](https://kind.sigs.k8s.io/docs/user/quick-start/#installation), then `kind create cluster`. A current kind release uses a new enough node image.
- **MicroK8s:** [install MicroK8s](https://microk8s.io/docs/getting-started) from the `1.36/stable` channel or newer, then `sudo microk8s enable rbac dns hostpath-storage`. Save its kubeconfig with `sudo microk8s config > ~/.kube/microk8s` and use it with `export KUBECONFIG=~/.kube/microk8s`.

Download the archive for your OS and CPU from the [latest release](https://github.com/luci1900/jk/releases/latest) and put `kubectl-jk` and `kubectl_complete-jk` on your PATH.

Then install jk in the cluster:
```
kubectl jk install
```

This installs the CRDs, the operator and the registry in `jk-system` from the release images on ghcr. Run it again to upgrade and `jk version` shows the versions of the CLI and the operator. `--image-repo` changes where the images come from.

`jk uninstall` destroys every model first, so the teardown hooks run, then removes jk. It asks first, `-y` skips that, `--destroy-storage` deletes the volumes (they are kept by default) and `--force` skips the graceful removal.

jk is a kubectl plugin, `kubectl jk <command>`. It uses your kubeconfig and the current kube context. A model is a namespace. `-m <model>` picks one, and the default is the context's namespace. `--help` on any command lists its flags. To type `jk` instead of `kubectl jk`, add `alias jk='kubectl jk'` to your shell, as the examples below do.

Tab completion works once kubectl's own completion is set up. For the alias in bash, also add `complete -o default -F __start_kubectl jk`.

## Try it

Deploy PostgreSQL and a charm that wants a database:

```
jk add-model demo
jk deploy postgresql-k8s --channel 14/stable --trust
jk deploy data-integrator --config database-name=test
jk integrate data-integrator postgresql-k8s
jk status --relations --watch
jk run data-integrator/leader get-credentials
jk ssh postgresql-k8s/0
jk destroy-model demo
```

`add-model` also switches your kube context to the new namespace. Wait in `status` until both applications are active, which takes a few minutes while the images download. `get-credentials` prints the database's endpoint, username and password. `destroy-model` waits for the teardown hooks, `--force` deletes the namespace at once, and volumes are kept unless you pass `--destroy-storage`.

## Models

```
jk models
jk switch demo
jk model-config
```

## Applications

```
jk deploy ./my.charm
jk config postgresql-k8s profile=testing
jk scale-application postgresql-k8s 1
jk refresh postgresql-k8s --channel 14/edge
jk remove-application postgresql-k8s
```

`--trust` gives the charm cluster access, and `--scope=namespace` narrows it. `jk find` and `info` search Charmhub.

## Offers

An offer lets other models relate to an application:

```
jk offer postgresql-k8s:database -m db
jk integrate data-integrator db.postgresql-k8s --alias pg
```

`offer --allow <model>,...` says which models may use it (`*` for all). The default comes from the model config `offer-allowed-models`.

## Day to day

```
jk exec --unit postgresql-k8s/0 -- ls /
jk resolved postgresql-k8s/0
jk show-unit postgresql-k8s/0
jk debug-log --include postgresql-k8s/0
jk set-constraints postgresql-k8s mem=4G
```

`resolved` retries a failed hook now, or skips it with `--no-retry`.

`ssh` and `scp` use `kubectl exec` and `kubectl cp` on the charm container (`--container` picks another), so they need `kubectl` on the PATH and `tar` in the container. With a terminal, `ssh` into the charm container passes your `TERM` and opens a login bash, and other containers get `/bin/sh`. `debug-log` follows the charm containers of the model's pods. `set-constraints` replaces the old constraints, as in juju. Put `-m` before the unit when you use `ssh`.

## YAML

Everything is a normal k8s object.

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

## More

- [docs/design.md](docs/design.md) is how it works.
- [docs/compat.md](docs/compat.md) is how it differs from juju, and what it does not support.
- [docs/dev.md](docs/dev.md) is for building and testing it yourself.
