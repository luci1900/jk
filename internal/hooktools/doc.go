// Package hooktools implements the hook-tool wire protocol and the thin multicall client, and the hook tools.
//
// The wire protocol and client (protocol.go) are written from scratch, not derived from juju's jujuc: see docs/design.md (Unit agent).
// The tools are in real.go (state, status, ports, config, logging), relation.go, secret.go, storage.go and cluster.go
// (network-get and goal-state). They run on a Backend that the agent provides per hook execution, buffer their writes
// until the hook succeeds, and are derived from juju's jujuc, therefore AGPL-3.0. Output formats, flag handling and error
// messages follow juju; ops' hookcmds is the conformance reference for what charms send and expect.
package hooktools
