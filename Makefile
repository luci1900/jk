# Dev tools (kind, ko, controller-gen, setup-envtest) are Go `tool` dependencies: `go tool <name>`.
export CGO_ENABLED = 0

VERSION      ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
CLUSTER      ?= jk-dev
ENVTEST_K8S  ?= 1.36.x
LDFLAGS      := -X github.com/luci1900/jk/internal/version.Version=$(VERSION)

# The charm-init image holds jk-agent and pebble (juju 4's pinned version): dev/agent.Dockerfile.
GOARCH         := $(shell go env GOARCH)
KIND_AGENT_IMAGE    = kind.local/jk-agent:$(VERSION)
AGENT_BUILD    = docker build --provenance=false --sbom=false --platform linux/$(GOARCH) -f dev/agent.Dockerfile --build-arg LDFLAGS='$(LDFLAGS)'

.PHONY: generate build install test test-envtest dev-cluster dev install-kind clean-cluster agent-image dev-microk8s install-microk8s operator-image

generate:
	cd dev && go generate ./...

build:
	go build -ldflags '$(LDFLAGS)' -o bin/kubectl-jk ./cmd/kubectl-jk

# Only the CLI: `jk install` then pulls the release images from ghcr. Use install-kind or install-microk8s to run your own build.
# Puts kubectl-jk in your Go bin (GOBIN, else GOPATH/bin), where kubectl finds it as `kubectl jk`, and kubectl_complete-jk, which kubectl runs to complete it.
install:
	go install -ldflags '$(LDFLAGS)' ./cmd/kubectl-jk ./cmd/kubectl_complete-jk

test:
	go test ./api/... ./internal/... ./cmd/... ./pkg/... ./dev/...

# Needs envtest binaries: downloaded by setup-envtest on first use.
test-envtest:
	KUBEBUILDER_ASSETS="$$(go tool setup-envtest use $(ENVTEST_K8S) -p path)" go test ./test/envtest/...

dev-cluster:
	go tool kind get clusters | grep -qx $(CLUSTER) || { go tool kind create cluster --name $(CLUSTER) --config dev/kind.yaml && rm -rf bin/stamp; }

# Builds the charm-init image and loads it into kind.
agent-image:
	$(AGENT_BUILD) -t $(KIND_AGENT_IMAGE) .
	go tool kind load docker-image --name $(CLUSTER) $(KIND_AGENT_IMAGE)

# kind: create the cluster if needed, build everything, load the images into kind (no registry) and install.
dev: install-kind
install-kind: dev-cluster build agent-image operator-image
	./bin/kubectl-jk --context kind-$(CLUSTER) install --operator-image kind.local/jk-operator:$(VERSION) --agent-image $(KIND_AGENT_IMAGE)
	# A dirty tree keeps the same image tag, so the pods must be told to start the image just loaded.
	kubectl --context kind-$(CLUSTER) -n jk-system rollout restart deploy/jk-operator
	kubectl --context kind-$(CLUSTER) -n jk-system rollout status deploy/jk-operator deploy/jk-registry --timeout=180s

# The operator image is built and loaded into kind (the slow part of `make dev`) only when the operator's Go sources, the version or the cluster changed.
OPERATOR_SRC := $(shell go list -deps -f '{{if not .Standard}}{{$$d := .Dir}}{{range .GoFiles}}{{$$d}}/{{.}} {{end}}{{end}}' ./cmd/jk-operator) go.mod go.sum
OPERATOR_STAMP := bin/stamp/operator-$(CLUSTER)-$(VERSION)
operator-image: $(OPERATOR_STAMP)
$(OPERATOR_STAMP): $(OPERATOR_SRC)
	KO_DOCKER_REPO=kind.local KIND_CLUSTER_NAME=$(CLUSTER) VERSION=$(VERSION) \
		go tool ko build --base-import-paths --tags $(VERSION) --platform linux/$$(go env GOARCH) ./cmd/jk-operator
	mkdir -p bin/stamp && touch $@

# MicroK8s (CI and local): push the images to its built-in registry addon (localhost:32000), enabling it if needed, and install.
# KUBE_CONTEXT is the MicroK8s kubeconfig context (`microk8s config > ~/.kube/config` makes it); the e2e tests read it from JK_E2E_CONTEXT.
MICROK8S_REGISTRY ?= localhost:32000
KUBE_CONTEXT      ?= microk8s
dev-microk8s: install-microk8s
install-microk8s: build
	@kubectl config get-contexts -o name | grep -qx '$(KUBE_CONTEXT)' || { echo "no kube context '$(KUBE_CONTEXT)': run 'microk8s config > ~/.kube/config' (or set KUBE_CONTEXT)" >&2; exit 1; }
	@curl -fsS -o /dev/null http://$(MICROK8S_REGISTRY)/v2/ 2>/dev/null || { \
		command -v microk8s >/dev/null || { echo "no registry at $(MICROK8S_REGISTRY): run 'microk8s enable registry'" >&2; exit 1; }; \
		microk8s enable registry && kubectl --context $(KUBE_CONTEXT) -n container-registry rollout status deploy/registry --timeout=300s; }
	$(AGENT_BUILD) -t $(MICROK8S_REGISTRY)/jk-agent:$(VERSION) .
	docker push $(MICROK8S_REGISTRY)/jk-agent:$(VERSION)
	KO_DOCKER_REPO=$(MICROK8S_REGISTRY) VERSION=$(VERSION) \
		go tool ko build --base-import-paths --insecure-registry --tags $(VERSION) --platform linux/$(GOARCH) ./cmd/jk-operator
	./bin/kubectl-jk --context $(KUBE_CONTEXT) install --operator-image $(MICROK8S_REGISTRY)/jk-operator:$(VERSION) --agent-image $(MICROK8S_REGISTRY)/jk-agent:$(VERSION)
	# A dirty tree keeps the same image tag, so the pods must be told to start the image just pushed.
	kubectl --context $(KUBE_CONTEXT) -n jk-system rollout restart deploy/jk-operator
	kubectl --context $(KUBE_CONTEXT) -n jk-system rollout status deploy/jk-operator deploy/jk-registry --timeout=180s

clean-cluster:
	go tool kind delete cluster --name $(CLUSTER)

.PHONY: charm test-charm test-e2e test-e2e-postgres test-e2e-cos

# Builds the test charms (bin/jk-test.charm, bin/jk-test-client.charm and bin/jk-test-rev2.charm, jk-test stamped as revision 2 for the refresh scenario) by hand; charmcraft needs LXD. Needs python3 with pip and network.
# Each is rebuilt only when its inputs change (the zip's digest then stays put, so the e2e run's push is a no-op too).
CHARM_FILES = $(shell find testcharms/$(1) -type f -not -path '*/tests/*' -not -path '*__pycache__*' -not -name '*.pyc') testcharms/build.sh Makefile
charm: bin/jk-test.charm bin/jk-test-client.charm bin/jk-test-rev2.charm
bin/jk-test.charm: $(call CHARM_FILES,jk-test)
	testcharms/build.sh jk-test $(shell go env GOARCH) bin/jk-test.charm
bin/jk-test-rev2.charm: $(call CHARM_FILES,jk-test)
	testcharms/build.sh jk-test $(shell go env GOARCH) bin/jk-test-rev2.charm 2
bin/jk-test-client.charm: $(call CHARM_FILES,jk-test-client)
	testcharms/build.sh jk-test-client $(shell go env GOARCH) bin/jk-test-client.charm

# Scenario tests of the test charms; needs python >= 3.10 with `pip install 'ops[testing]' pytest` (set PYTHON to a venv's python).
PYTHON ?= python3
test-charm:
	cd testcharms/jk-test && $(PYTHON) -m pytest -q tests
	cd testcharms/jk-test-client && $(PYTHON) -m pytest -q tests

# Needs `make dev` (kind-jk-dev with the operator running). Pushes the test charm to jk-registry and deploys it in throwaway e2e-* namespaces.
# The scenarios run E2E_PARALLEL at a time, each in its own namespace.
E2E_PARALLEL ?= 4
test-e2e: build charm
	go test -tags e2e -count=1 -timeout 30m -parallel $(E2E_PARALLEL) -v ./test/e2e/...

# The postgresql-k8s HA scenarios (Charmhub, large images, several minutes); also needs `make dev` and network.
test-e2e-postgres: build charm
	go test -tags e2e,postgres -count=1 -timeout 90m -v -run 'TestPostgres' ./test/e2e/...

# COS Lite offered to postgresql-k8s's model; the charms are amd64 only (CI), also needs `make dev` and network.
test-e2e-cos: build charm
	go test -tags e2e,postgres,cos -count=1 -timeout 90m -v -run 'TestCOS' ./test/e2e/...
