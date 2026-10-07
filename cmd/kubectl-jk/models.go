package main

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/luci1900/jk/pkg/sdk"
)

func (c *cli) modelCommands() []*cobra.Command {
	var noSwitch bool
	add := &cobra.Command{
		Use:   "add-model <name>",
		Short: "Create a model (a namespace) and switch to it",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cl, err := c.sdk(cmd.Context())
			if err != nil {
				return err
			}
			if err := cl.AddModel(cmd.Context(), args[0]); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Added model %q\n", args[0])
			if noSwitch {
				return nil
			}
			return c.switchTo(cmd, args[0])
		},
	}
	add.Flags().BoolVar(&noSwitch, "no-switch", false, "do not switch to the new model")

	var force, destroyStorage, releaseStorage, noPrompt, noWait bool
	var timeout string
	destroy := &cobra.Command{
		Use:   "destroy-model <name>",
		Short: "Remove a model: its relations and applications, then the namespace",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if destroyStorage && releaseStorage {
				return fmt.Errorf("--destroy-storage and --release-storage cannot be used together")
			}
			cl, err := c.sdk(cmd.Context())
			if err != nil {
				return err
			}
			if !noPrompt {
				what := "Destroying the model removes all its applications and relations"
				if destroyStorage {
					what += " and deletes their storage"
				} else {
					what += "; storage is kept (use --destroy-storage to delete it)"
				}
				ok, err := c.confirm(cmd, fmt.Sprintf("WARNING! %s.\nDestroy model %q?", what, args[0]))
				if err != nil {
					return err
				}
				if !ok {
					return fmt.Errorf("model not destroyed")
				}
			}
			o := sdk.DestroyOptions{Force: force, DestroyStorage: destroyStorage, NoWait: noWait}
			if timeout != "" {
				if o.Timeout, err = parseDuration(timeout); err != nil {
					return err
				}
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Destroying model %q\n", args[0])
			if err := cl.DestroyModel(cmd.Context(), args[0], o); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Model %q destroyed\n", args[0])
			return nil
		},
	}
	destroy.Flags().BoolVar(&force, "force", false, "skip the graceful removal and delete the namespace at once")
	destroy.Flags().BoolVar(&destroyStorage, "destroy-storage", false, "delete the volumes of the model's applications")
	destroy.Flags().BoolVar(&releaseStorage, "release-storage", false, "keep the volumes (the default)")
	destroy.Flags().BoolVarP(&noPrompt, "no-prompt", "y", false, "do not ask for confirmation")
	destroy.Flags().BoolVar(&noWait, "no-wait", false, "return once the namespace is deleted, without waiting for it to disappear")
	destroy.Flags().StringVar(&timeout, "timeout", "", "how long to wait for the graceful removal (default 10m)")

	format := new(string)
	models := &cobra.Command{
		Use:     "models",
		Aliases: []string{"list-models"},
		Short:   "List models",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cl, err := c.sdk(cmd.Context())
			if err != nil {
				return err
			}
			ms, err := cl.Models(cmd.Context())
			if err != nil {
				return err
			}
			current, _ := c.model()
			if done, err := printStructured(cmd.OutOrStdout(), *format, map[string]any{"models": ms, "current-model": current}); done {
				return err
			}
			if len(ms) == 0 {
				fmt.Fprintln(cmd.ErrOrStderr(), "No models: add one with `jk add-model <name>`.")
				return nil
			}
			tw := newTable(cmd.OutOrStdout())
			fmt.Fprintln(tw, "Model\tStatus\tApplications")
			for _, m := range ms {
				name := m.Name
				if name == current {
					name += "*"
				}
				fmt.Fprintf(tw, "%s\t%s\t%d\n", name, m.Status, m.Applications)
			}
			return tw.Flush()
		},
	}
	format = formatFlag(models, formatTabular)

	switchCmd := &cobra.Command{
		Use:   "switch [model]",
		Short: "Show or set the current model (the kube context's namespace)",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				ns, err := c.model()
				if err != nil {
					return err
				}
				fmt.Fprintln(cmd.OutOrStdout(), ns)
				return nil
			}
			cl, err := c.sdk(cmd.Context())
			if err != nil {
				return err
			}
			ms, err := cl.Models(cmd.Context())
			if err != nil {
				return err
			}
			known := false
			for _, m := range ms {
				known = known || m.Name == args[0]
			}
			if !known {
				return fmt.Errorf("model %q not found", args[0])
			}
			return c.switchTo(cmd, args[0])
		},
	}

	var reset []string
	cfgCmd := &cobra.Command{
		Use:   "model-config [key[=value]...]",
		Short: "Show or set model config",
		RunE: func(cmd *cobra.Command, args []string) error {
			cl, err := c.sdk(cmd.Context())
			if err != nil {
				return err
			}
			if len(reset) > 0 {
				kv := map[string]string{}
				for _, k := range reset {
					kv[k] = ""
				}
				if err := cl.SetModelConfig(cmd.Context(), kv); err != nil {
					return err
				}
				if len(args) == 0 {
					return nil
				}
			}
			if len(args) > 0 && strings.Contains(args[0], "=") {
				kv, err := keyValues(args)
				if err != nil {
					return err
				}
				return cl.SetModelConfig(cmd.Context(), kv)
			}
			cfg, err := cl.ModelConfig(cmd.Context())
			if err != nil {
				return err
			}
			switch {
			case len(args) > 1:
				return fmt.Errorf("give one key to show, or key=value pairs to set")
			case len(args) == 1:
				v, ok := cfg[args[0]]
				if !ok {
					return fmt.Errorf("model config key %q not found", args[0])
				}
				fmt.Fprintln(cmd.OutOrStdout(), v)
				return nil
			}
			tw := newTable(cmd.OutOrStdout())
			fmt.Fprintln(tw, "Key\tValue")
			for _, k := range sortedKeys(cfg) {
				fmt.Fprintf(tw, "%s\t%s\n", k, cfg[k])
			}
			return tw.Flush()
		},
	}
	cfgCmd.Flags().StringSliceVar(&reset, "reset", nil, "keys to reset to their defaults")
	return []*cobra.Command{add, destroy, models, switchCmd, cfgCmd}
}

// switchTo sets the namespace of the kube context in use, so kubectl follows.
func (c *cli) switchTo(cmd *cobra.Command, model string) error {
	loader := c.flags.ToRawKubeConfigLoader()
	raw, err := loader.RawConfig()
	if err != nil {
		return err
	}
	name := raw.CurrentContext
	if c.flags.Context != nil && *c.flags.Context != "" {
		name = *c.flags.Context
	}
	kctx := raw.Contexts[name]
	if kctx == nil {
		return fmt.Errorf("kube context %q not found", name)
	}
	prev := kctx.Namespace
	if prev == "" {
		prev = "default"
	}
	kctx.Namespace = model
	if err := clientcmd.ModifyConfig(loader.ConfigAccess(), raw, true); err != nil {
		return fmt.Errorf("saving the kubeconfig: %w", err)
	}
	fmt.Fprintf(cmd.OutOrStdout(), "%s -> %s\n", prev, model)
	return nil
}
