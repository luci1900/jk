# The charm-init image: jk-agent and pebble (juju 4's pinned version) on an empty base. Build from the repository root:
#   docker build -f dev/agent.Dockerfile --build-arg LDFLAGS=... -t jk-agent .
FROM --platform=$BUILDPLATFORM golang:1.26 AS build
ARG TARGETOS TARGETARCH
ARG PEBBLE_VERSION=v1.32.1
ARG LDFLAGS
ENV CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go build -ldflags "$LDFLAGS" -o /out/jk-agent ./cmd/jk-agent
# A cross-compiled `go install` puts the binary in $GOPATH/bin/<os>_<arch>/.
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    GOBIN= go install github.com/canonical/pebble/cmd/pebble@$PEBBLE_VERSION && \
    cp "$(go env GOPATH)/bin/${GOOS}_${GOARCH}/pebble" /out/pebble 2>/dev/null || cp "$(go env GOPATH)/bin/pebble" /out/pebble

FROM scratch
COPY --from=build /out/jk-agent /out/pebble /
ENTRYPOINT ["/jk-agent"]
