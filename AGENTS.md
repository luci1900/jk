# AGENTS.md

jk runs juju sidecar charms on plain Kubernetes. Read `README.md`, `docs/compat.md` and `docs/design.md` first, and `docs/plan.md` for what is left. If a design decision looks wrong, say so and ask before changing it.

## Testing

See the Development section of `README.md` for the dev cluster and the test commands.

## Rules

- Before designing something, check how juju does it on k8s (checkout at `../juju`) and copy it unless a k8s-native way is clearly better. Code copied from juju needs a note on where it came from.
- Docs are brief and plain, with one line per paragraph or bullet.
- Keep `README.md`, `docs/compat.md`, `docs/design.md` and `docs/plan.md` current.
