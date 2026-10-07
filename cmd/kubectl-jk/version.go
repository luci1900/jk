package main

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/luci1900/jk/internal/version"
	"github.com/luci1900/jk/pkg/sdk"
)

func (c *cli) versionCmd() *cobra.Command {
	var clientOnly bool
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Print the CLI's version and the installed operator's",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			out := cmd.OutOrStdout()
			fmt.Fprintf(out, "client: %s\n", version.Version)
			if clientOnly {
				return
			}
			fmt.Fprintln(out, "operator:", c.operatorVersion(cmd))
		},
	}
	cmd.Flags().BoolVar(&clientOnly, "client", false, "print only the CLI's version, without asking the cluster")
	return cmd
}

// operatorVersion describes the installed operator in one line. A cluster that cannot be reached is not an error here, so
// the client's version is always printed.
func (c *cli) operatorVersion(cmd *cobra.Command) string {
	cl, err := c.sdk(cmd.Context())
	var info *sdk.OperatorInfo
	if err == nil {
		info, err = cl.OperatorVersion(cmd.Context())
	}
	switch {
	case errors.Is(err, sdk.ErrNotInstalled):
		return "not installed (run `kubectl jk install`)"
	case err != nil:
		return fmt.Sprintf("unknown (%v)", err)
	}
	v := info.Version
	if v == "" {
		v = "unknown"
	}
	line := fmt.Sprintf("%s (%s, %d/%d ready)", v, info.Image, info.Ready, info.Desired)
	if info.Version != version.Version {
		line += "; differs from the client, `kubectl jk install` upgrades it"
	}
	return line
}
