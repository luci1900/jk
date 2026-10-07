package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"sigs.k8s.io/yaml"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/pkg/sdk"
)

func (c *cli) appCommands() []*cobra.Command {
	return []*cobra.Command{c.deployCmd(), c.refreshCmd(), c.removeApplicationCmd(), c.configCmd(), c.trustCmd(),
		c.scaleCmd(), c.addUnitCmd(), c.removeUnitCmd()}
}

// trustFor maps the juju flags to a trust level: --trust alone means cluster scope, as in juju.
func trustFor(trust bool, scope string) (v1alpha1.Trust, error) {
	switch {
	case !trust && scope != "":
		return "", fmt.Errorf("--scope applies to --trust")
	case !trust:
		return v1alpha1.TrustNone, nil
	}
	switch scope {
	case "", "cluster":
		return v1alpha1.TrustCluster, nil
	case "namespace":
		return v1alpha1.TrustNamespace, nil
	}
	return "", fmt.Errorf("invalid --scope %q: want cluster or namespace", scope)
}

func (c *cli) deployCmd() *cobra.Command {
	var o sdk.DeployOptions
	var revision int
	var config, resources, storage []string
	var constraints, scope string
	var trust bool
	cmd := &cobra.Command{
		Use:   "deploy <charm> [application-name]",
		Short: "Deploy a charm from Charmhub or a local .charm file",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			o.Charm = args[0]
			if len(args) == 2 {
				o.Name = args[1]
			}
			var err error
			if cmd.Flags().Changed("revision") {
				o.Revision = &revision
			}
			if o.Trust, err = trustFor(trust, scope); err != nil {
				return err
			}
			if o.Config, err = keyValues(config); err != nil {
				return err
			}
			if o.Resources, err = keyValues(resources); err != nil {
				return err
			}
			if o.Storage, err = sdk.ParseStorage(storage); err != nil {
				return err
			}
			if o.Constraints, err = sdk.ParseConstraints(constraints); err != nil {
				return err
			}
			cl, err := c.sdk(cmd.Context())
			if err != nil {
				return err
			}
			res, err := cl.Deploy(cmd.Context(), o)
			if err != nil {
				return err
			}
			fmt.Fprintln(cmd.OutOrStdout(), res)
			return nil
		},
	}
	f := cmd.Flags()
	f.StringVar(&o.Channel, "channel", "", "channel of a Charmhub charm (default latest/stable)")
	f.IntVar(&revision, "revision", 0, "revision of a Charmhub charm")
	f.StringVar(&o.Base, "base", "", "base of a Charmhub charm, e.g. ubuntu@22.04")
	f.StringArrayVar(&config, "config", nil, "config option as key=value (repeatable)")
	f.IntVarP(&o.Scale, "num-units", "n", 1, "number of units")
	f.StringArrayVar(&resources, "resource", nil, "resource as name=image (repeatable)")
	f.BoolVar(&trust, "trust", false, "grant the charm access to the cluster (--scope narrows it)")
	f.StringVar(&scope, "scope", "", "with --trust: cluster (default) or namespace")
	f.StringArrayVar(&storage, "storage", nil, "storage as name=size or name=class,size (repeatable)")
	f.StringVar(&constraints, "constraints", "", "constraints: mem, cpu-power, arch")
	return cmd
}

func (c *cli) refreshCmd() *cobra.Command {
	var o sdk.RefreshOptions
	var revision int
	cmd := &cobra.Command{
		Use:   "refresh <application>",
		Short: "Upgrade an application's charm",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if cmd.Flags().Changed("revision") {
				o.Revision = &revision
			}
			cl, err := c.sdk(cmd.Context())
			if err != nil {
				return err
			}
			res, err := cl.Refresh(cmd.Context(), args[0], o)
			if err != nil {
				return err
			}
			fmt.Fprintln(cmd.OutOrStdout(), res)
			return nil
		},
	}
	cmd.Flags().StringVar(&o.Channel, "channel", "", "move to this channel")
	cmd.Flags().IntVar(&revision, "revision", 0, "move to this revision")
	cmd.Flags().StringVar(&o.Path, "path", "", "use this local .charm file")
	return cmd
}

func (c *cli) removeApplicationCmd() *cobra.Command {
	var destroyStorage, noWait bool
	cmd := &cobra.Command{
		Use:   "remove-application <application>...",
		Short: "Remove applications (their volumes are kept unless --destroy-storage)",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cl, err := c.sdk(cmd.Context())
			if err != nil {
				return err
			}
			for _, app := range args {
				if err := cl.RemoveApplication(cmd.Context(), app, destroyStorage); err != nil {
					return err
				}
				fmt.Fprintf(cmd.OutOrStdout(), "will remove application %s\n", app)
			}
			if noWait {
				return nil
			}
			for _, app := range args {
				if err := cl.WaitRemoved(cmd.Context(), app, 10*time.Minute); err != nil {
					return err
				}
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&destroyStorage, "destroy-storage", false, "delete the application's volumes")
	cmd.Flags().Bool("release-storage", false, "keep the application's volumes (the default)")
	cmd.Flags().BoolVar(&noWait, "no-wait", false, "return without waiting for the removal")
	return cmd
}

func (c *cli) configCmd() *cobra.Command {
	var reset []string
	format := new(string)
	cmd := &cobra.Command{
		Use:   "config <application> [key | key=value ...]",
		Short: "Show or change an application's config",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cl, err := c.sdk(cmd.Context())
			if err != nil {
				return err
			}
			app, rest := args[0], args[1:]
			if len(reset) > 0 || (len(rest) > 0 && strings.Contains(rest[0], "=")) {
				if len(reset) > 0 {
					if err := cl.ResetConfig(cmd.Context(), app, reset); err != nil {
						return err
					}
				}
				if len(rest) > 0 {
					kv, err := keyValues(rest)
					if err != nil {
						return err
					}
					return cl.SetConfig(cmd.Context(), app, kv)
				}
				return nil
			}
			view, err := cl.Config(cmd.Context(), app)
			if err != nil {
				return err
			}
			if len(rest) == 1 {
				s, ok := view.Settings[rest[0]]
				if !ok {
					return fmt.Errorf("unknown option %q", rest[0])
				}
				fmt.Fprintln(cmd.OutOrStdout(), s.Value)
				return nil
			}
			if len(rest) > 1 {
				return fmt.Errorf("give one key to show, or key=value pairs to set")
			}
			if *format == formatJSON {
				_, err := printStructured(cmd.OutOrStdout(), formatJSON, view)
				return err
			}
			b, err := yaml.Marshal(view)
			if err != nil {
				return err
			}
			fmt.Fprint(cmd.OutOrStdout(), string(b))
			return nil
		},
	}
	cmd.Flags().StringSliceVar(&reset, "reset", nil, "options to return to their defaults")
	format = formatFlag(cmd, formatYAML)
	return cmd
}

func (c *cli) trustCmd() *cobra.Command {
	var remove bool
	var scope string
	cmd := &cobra.Command{
		Use:   "trust <application>",
		Short: "Grant or remove an application's access to the cluster",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			level, err := trustFor(!remove, scope)
			if err != nil {
				return err
			}
			cl, err := c.sdk(cmd.Context())
			if err != nil {
				return err
			}
			return cl.SetTrust(cmd.Context(), args[0], level)
		},
	}
	cmd.Flags().BoolVar(&remove, "remove", false, "remove the access")
	cmd.Flags().StringVar(&scope, "scope", "", "cluster (default) or namespace")
	return cmd
}

func (c *cli) scaleCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "scale-application <application> <units>",
		Short: "Set the number of units",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			n, err := strconv.Atoi(args[1])
			if err != nil {
				return fmt.Errorf("invalid number of units %q", args[1])
			}
			cl, err := c.sdk(cmd.Context())
			if err != nil {
				return err
			}
			if err := cl.Scale(cmd.Context(), args[0], n); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "%s scaled to %d units\n", args[0], n)
			return nil
		},
	}
}

func (c *cli) addUnitCmd() *cobra.Command {
	var n int
	cmd := &cobra.Command{
		Use:   "add-unit <application>",
		Short: "Add units to an application",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cl, err := c.sdk(cmd.Context())
			if err != nil {
				return err
			}
			scale, err := cl.AddUnits(cmd.Context(), args[0], n)
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "%s scaled to %d units\n", args[0], scale)
			return nil
		},
	}
	cmd.Flags().IntVarP(&n, "num-units", "n", 1, "number of units to add")
	return cmd
}

func (c *cli) removeUnitCmd() *cobra.Command {
	var n int
	cmd := &cobra.Command{
		Use:   "remove-unit <application>",
		Short: "Remove units from an application (the highest numbers go first)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.Contains(args[0], "/") {
				return fmt.Errorf("units are removed by count on Kubernetes (the highest numbers first): jk remove-unit %s --num-units N", strings.Split(args[0], "/")[0])
			}
			cl, err := c.sdk(cmd.Context())
			if err != nil {
				return err
			}
			scale, err := cl.RemoveUnits(cmd.Context(), args[0], n)
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "%s scaled to %d units\n", args[0], scale)
			return nil
		},
	}
	cmd.Flags().IntVar(&n, "num-units", 1, "number of units to remove")
	return cmd
}

// sortedKeys returns the keys of m in order.
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
