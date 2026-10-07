package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/luci1900/jk/internal/agent"
)

// runUnit runs the unit agent (a Pebble service of the charm container; see init.go for the layer).
//
// Flags: --data-dir (default /var/lib/juju), --containers-dir (default /charm/containers).
// Environment, set by the operator on the charm container: JK_POD_NAME, JK_NAMESPACE, JK_MODEL_NAME,
// JK_MODEL_UUID, JK_APP and JUJU_CONTAINER_NAMES; the API server is reached with the in-cluster config.
func runUnit(args []string) error {
	fs := flag.NewFlagSet("unit", flag.ExitOnError)
	dataDir := fs.String("data-dir", "/var/lib/juju", "")
	containersDir := fs.String("containers-dir", "/charm/containers", "")
	fs.Parse(args)

	id, err := agent.IdentityFromEnv(os.Getenv)
	if err != nil {
		return err
	}
	self, err := os.Executable()
	if err != nil {
		return err
	}
	// SIGTERM is the termination notice: Run decides whether the unit comes back or goes away.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	return agent.Run(ctx, agent.RunOptions{
		Identity:      id,
		DataDir:       *dataDir,
		ContainersDir: *containersDir,
		Self:          self,
	})
}
