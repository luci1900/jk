// Command jk-agent is the per-pod unit agent and, via multicall, the hook tools.
//
//	jk-agent init   init container: copy binaries, write the Pebble layer, fetch the charm
//	jk-agent unit   run the unit agent (resolver, hook execution, leadership; see internal/agent)
//	<tool>          hook tool, when invoked through a symlink named after the tool
package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/luci1900/jk/internal/hooktools"
)

func main() {
	name := filepath.Base(os.Args[0])
	if name != "jk-agent" {
		os.Exit(hooktools.Main(name, os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
	}
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: jk-agent init|unit")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "init":
		err = runInit(os.Args[2:])
	case "unit":
		err = runUnit(os.Args[2:])
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "jk-agent:", err)
		os.Exit(1)
	}
}

// unitNames derives the juju unit name and tag from a StatefulSet pod name (<app>-<n>).
func unitNames(pod string) (app, unit, tag string, err error) {
	for i := len(pod) - 1; i > 0; i-- {
		if pod[i] == '-' {
			app, n := pod[:i], pod[i+1:]
			return app, app + "/" + n, "unit-" + app + "-" + n, nil
		}
	}
	return "", "", "", fmt.Errorf("pod name %q is not <app>-<ordinal>", pod)
}
