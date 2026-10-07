# jk design

jk runs juju sidecar charms on Kubernetes without a juju controller. Charms see what they would see under juju 4, but all state lives in k8s objects. The only things jk runs in the cluster are an operator and an agent in each charm pod.

## Parts

- **CRDs:** `Application`, `Relation`, `Action`, `UnitData`, `AppData`, `Offer` and `RemoteData`, all namespaced.
- **`jk-operator`:** a Deployment in `jk-system`. It turns Applications into StatefulSets, validates relations, picks leaders, turns secret grants into RBAC and runs teardown in order.
- **`jk-registry`:** an OCI registry (zot) in `jk-system` that caches charms.
- **Unit agent:** runs in each charm pod. It uses juju's uniter resolver, ported to Go, so hooks run in juju's order. It serves the hook tools to the charm.
- **Admission policies:** CEL policies that decide who may write what.
- **CLI:** `kubectl jk`, built on `pkg/sdk`. It only talks to the k8s API.

| juju | jk |
|---|---|
| controller | `jk-system` and the cluster's API server |
| model | namespace |
| model config | `jk-model` ConfigMap |
| application | `Application` and a StatefulSet |
| unit | pod `<app>-<n>` |
| relation | `Relation` |
| relation data | `UnitData` and `AppData` |
| leadership | a `Lease`, assigned by the operator |
| secrets | k8s Secrets |
| actions | `Action` |
| storage | the StatefulSet's volume claims |
| `trust` | a Role or ClusterRole for the app's ServiceAccount |
| users | k8s users and RBAC |
| charm store | `jk-registry` (a cache of Charmhub) |

## State

- `UnitData/<app>-<n>` is written only by that unit. It holds the unit's relation data, state, status and opened ports.
- `AppData/<app>` is written only by the leader. It holds the app's relation data and status.
- Admission policies check the pod name in the agent's token, so a unit can only write its own data.
- A hook's writes are committed only if the hook succeeds. A crash reruns the hook, as in juju.
- The model UUID is the namespace UID and the unit number is the pod ordinal. Relation and action ids come from a counter in the `jk-model` ConfigMap.

## Charms and pods

- An Application only needs the charm name. The operator resolves the rest through Charmhub and records it in `status`. It never upgrades on its own: changing the channel or revision is a refresh, which rolls the pods.
- The operator downloads charms into `jk-registry`. The init container pulls the charm by digest and unpacks it, so nodes never pull charm images. The CLI pushes local charms there too.
- The pod layout copies juju's, because charms depend on it. Charms also patch these objects (postgresql-k8s sets the rollout partition), so the operator uses server-side apply and only owns its own fields.
- As in juju, removed units keep their volumes (the operator sets the PVs to `Retain`) until you ask for `--destroy-storage`.

## Relations, secrets and actions

- The operator validates a Relation the way juju does and creates the peer relations itself. Deleting a Relation waits until both sides have run `relation-departed` and `relation-broken`.
- Each secret revision is an immutable k8s Secret. A grant becomes a Role for the consumer app's ServiceAccount. RBAC enforces grants per app and the agent enforces them per unit.
- An `Action` is a juju task. The agent runs actions between hooks. `exec` is an action named `juju-exec`, as in juju.

## Offers

An `Offer` names an application, its endpoints and the models (namespaces) allowed to use it. A consumer relates to it with a normal Relation that names the offer. An admission policy allows that if the consumer's namespace is allowed, or if the person relating could create relations in the offering namespace anyway.

The operator mirrors the Relation into the offering namespace and copies relation data and granted secrets across as `RemoteData` objects. Agents never read another namespace.

## Notes

jk needs Kubernetes 1.36, for MutatingAdmissionPolicy. The CRDs are `v1alpha1` with no conversion, so a breaking change means reinstalling. The repository is AGPL-3.0, like juju, so code ported from juju can live anywhere with a note on where it came from.
