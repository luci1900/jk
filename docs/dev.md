# Development

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
