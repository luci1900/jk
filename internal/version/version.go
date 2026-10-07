// Package version holds build-time constants, set with -ldflags or ko's env.
package version

// Version is the jk release, also the default tag of jk's images.
var Version = "dev"

// ImageRepo is the default repository for jk-operator and jk-agent images.
var ImageRepo = "ghcr.io/luci1900"
