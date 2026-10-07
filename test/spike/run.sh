#!/bin/sh
# Layout spike: build, load, push the charm, apply, wait for the install hook. Needs `make dev` (cluster kind-jk-dev) and PEBBLE=<linux/arm64 pebble binary>, CHARM=<postgresql-k8s .charm>.
set -eu
cd "$(dirname "$0")"
CTX=kind-jk-dev
NS=${NS:-jk-spike}
ST=$(mktemp -d)
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o "$ST/jk-agent" ../../cmd/jk-agent
cp "$PEBBLE" "$ST/pebble"
cp Dockerfile "$ST/"
docker build -q -t kind.local/jk-spike-agent:dev "$ST"
go tool kind load docker-image --name jk-dev kind.local/jk-spike-agent:dev
# push the charm as a single-layer image through a port-forward
kubectl --context $CTX -n jk-system port-forward svc/jk-registry 15000:5000 >/dev/null &
PF=$!
trap 'kill $PF' EXIT
sleep 2
DIGEST=$(CGO_ENABLED=0 go run ./push "$CHARM" localhost:15000/charms/postgresql-k8s:959)
echo "charm digest $DIGEST"
kubectl --context $CTX create namespace $NS --dry-run=client -o yaml | kubectl --context $CTX apply -f -
sed "s|@CHARM_DIGEST@|$DIGEST|g; s|@NAMESPACE_UID@|$(kubectl --context $CTX get ns $NS -o jsonpath='{.metadata.uid}')|g" statefulset.yaml | kubectl --context $CTX -n $NS apply -f -
