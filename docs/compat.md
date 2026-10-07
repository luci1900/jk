# Compared with juju

## For users

Commands keep juju's names and flags. These are the differences:

- There are no controllers, clouds, credentials or juju users. `jk install` replaces `bootstrap`, the kube context picks the cluster and access is k8s RBAC.
- The current model is the kube context's namespace, so `switch` changes it for kubectl too.
- `-m` picks the model, as in juju. kubectl's `--namespace` also works, but `-n` is only the number of units.
- Offers use `integrate <app> <model>.<offer>`. There is no `consume` or `remove-saas`. Access is per model, not per user, and only within one cluster.
- `status` has no controller or cloud columns, and it has `--watch`. It colours states like juju does on a terminal (`--color` and `--no-color` override, as does `NO_COLOR`), and takes names or globs such as `jk status "pg*"`.

## For charms

- `config-changed` also runs when trust changes, but not when the address changes.
- `network-get` reports the pod address with no FQDN.
- `JUJU_VERSION` is 3.6.x, or 4.0.x when the charm's `assumes` excludes 3.6.
- The leader is the first unit to start, which is not always unit 0.
- A unit runs one action at a time. `parallel` and `execution-group` are ignored.
- Relation data is limited to about 1.5 MiB per unit, because it is an etcd object.

## Not supported

These juju features are not supported:

- Commands: the secrets commands, `resources`, `attach-resource`, `upgrade-model`, `expose` and `debug-hooks`. The `show-*` commands leave out relation data.
- The hook tools `juju-reboot`, `credential-get` and `resource-get` (file resources), secret rotation and Pebble notices. The three hook tools return errors.
- The constraint `tags`.
- Offers: `relation-model-get` for the other model of an offer, suspending a connection from the offering side and offers of peer endpoints. Offers only work within one cluster.
- Bundles, your own charm registry and air-gapped clusters.
- The juju API (the stock `juju` CLI, terraform-provider-juju and libjuju) and migrating juju models.
