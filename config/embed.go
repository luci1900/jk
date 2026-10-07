// Package config embeds the generated CRDs and the base install manifests.
package config

import "embed"

//go:embed crd/*.yaml base/*.yaml
var FS embed.FS
