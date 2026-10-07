package main

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/luci1900/jk/pkg/sdk"
)

func (c *cli) showCommands() []*cobra.Command {
	return []*cobra.Command{c.showApplicationCmd(), c.showUnitCmd(), c.showModelCmd(), c.constraintsCmd(), c.setConstraintsCmd()}
}

// printInfo writes v as YAML, or as JSON with --format json (the show commands have no table).
func printInfo(cmd *cobra.Command, format string, v any) error {
	if format == formatTabular || format == "" {
		format = formatYAML
	}
	_, err := printStructured(cmd.OutOrStdout(), format, v)
	return err
}

func (c *cli) showApplicationCmd() *cobra.Command {
	format := new(string)
	cmd := &cobra.Command{
		Use:   "show-application <application>...",
		Short: "Show an application's charm, constraints, endpoints and status",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cl, err := c.sdk(cmd.Context())
			if err != nil {
				return err
			}
			out := map[string]*sdk.ApplicationInfo{}
			for _, name := range args {
				if out[name], err = cl.ShowApplication(cmd.Context(), name); err != nil {
					return err
				}
			}
			return printInfo(cmd, *format, out)
		},
	}
	format = formatFlag(cmd, formatYAML)
	return cmd
}

func (c *cli) showUnitCmd() *cobra.Command {
	format := new(string)
	cmd := &cobra.Command{
		Use:   "show-unit <unit>...",
		Short: "Show a unit's pod, address, ports and status",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cl, err := c.sdk(cmd.Context())
			if err != nil {
				return err
			}
			out := map[string]*sdk.UnitInfo{}
			for _, name := range args {
				if out[name], err = cl.ShowUnit(cmd.Context(), name); err != nil {
					return err
				}
			}
			return printInfo(cmd, *format, out)
		},
	}
	format = formatFlag(cmd, formatYAML)
	return cmd
}

func (c *cli) showModelCmd() *cobra.Command {
	format := new(string)
	cmd := &cobra.Command{
		Use:   "show-model",
		Short: "Show the model's version, config and size",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cl, err := c.sdk(cmd.Context())
			if err != nil {
				return err
			}
			info, err := cl.ShowModel(cmd.Context())
			if err != nil {
				return err
			}
			return printInfo(cmd, *format, map[string]*sdk.ModelInfo{info.Name: info})
		},
	}
	format = formatFlag(cmd, formatYAML)
	return cmd
}

func (c *cli) constraintsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "constraints <application>",
		Short: "Show an application's constraints",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cl, err := c.sdk(cmd.Context())
			if err != nil {
				return err
			}
			cons, err := cl.Constraints(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			fmt.Fprintln(cmd.OutOrStdout(), sdk.FormatConstraints(cons))
			return nil
		},
	}
}

func (c *cli) setConstraintsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "set-constraints <application> [constraint=value ...]",
		Short: "Replace an application's constraints (none clears them)",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cons, err := sdk.ParseConstraints(strings.Join(args[1:], " "))
			if err != nil {
				return err
			}
			cl, err := c.sdk(cmd.Context())
			if err != nil {
				return err
			}
			return cl.SetConstraints(cmd.Context(), args[0], cons)
		},
	}
}
