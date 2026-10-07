package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericclioptions"

	"github.com/luci1900/jk/internal/install"
	"github.com/luci1900/jk/internal/version"
	"github.com/luci1900/jk/pkg/sdk"
)

func newInstallCmd(flags *genericclioptions.ConfigFlags) *cobra.Command {
	var imageRepo, operatorImage, agentImage string
	cmd := &cobra.Command{
		Use:   "install",
		Short: "Install or upgrade jk in the cluster (idempotent)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			c, err := newClient(flags)
			if err != nil {
				return err
			}
			if operatorImage == "" {
				operatorImage = fmt.Sprintf("%s/jk-operator:%s", imageRepo, version.Version)
			}
			if agentImage == "" {
				agentImage = fmt.Sprintf("%s/jk-agent:%s", imageRepo, version.Version)
			}
			if err := install.Apply(cmd.Context(), c, install.Options{OperatorImage: operatorImage, AgentImage: agentImage}); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "jk %s installed (operator image %s)\n", version.Version, operatorImage)
			return nil
		},
	}
	cmd.Flags().StringVar(&imageRepo, "image-repo", version.ImageRepo, "repository holding the jk-operator and jk-agent images")
	cmd.Flags().StringVar(&agentImage, "agent-image", "", "full charm-init (jk-agent + pebble) image reference (overrides --image-repo)")
	cmd.Flags().StringVar(&operatorImage, "operator-image", "", "full operator image reference (overrides --image-repo)")
	return cmd
}

// uninstallCmd destroys every model first, while the operator is still there to run the teardown hooks and finalizers;
// without it they would hang once the operator and the CRDs are gone.
func (c *cli) uninstallCmd() *cobra.Command {
	var force, destroyStorage, noPrompt bool
	var timeout string
	cmd := &cobra.Command{
		Use:   "uninstall",
		Short: "Destroy every model, then delete jk-system and the CRDs",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			kc, err := c.installClient()
			if err != nil {
				return err
			}
			opts := sdk.DestroyOptions{Force: force, DestroyStorage: destroyStorage}
			if timeout != "" {
				if opts.Timeout, err = parseDuration(timeout); err != nil {
					return err
				}
			}
			cl, err := c.sdk(cmd.Context())
			if err != nil {
				return err
			}
			models, err := cl.Models(cmd.Context())
			if err != nil {
				return err
			}
			if len(models) > 0 {
				if !noPrompt {
					what := "storage is kept (use --destroy-storage to delete it)"
					if destroyStorage {
						what = "their storage is deleted"
					}
					var list []string
					for _, m := range models {
						list = append(list, fmt.Sprintf("  %s (%d applications)", m.Name, m.Applications))
					}
					ok, err := c.confirm(cmd, fmt.Sprintf("WARNING! Uninstalling jk destroys all models and their applications; %s:\n%s\nUninstall jk?", what, strings.Join(list, "\n")))
					if err != nil {
						return err
					}
					if !ok {
						return fmt.Errorf("jk not uninstalled")
					}
				}
				for _, m := range models {
					fmt.Fprintf(cmd.OutOrStdout(), "Destroying model %q\n", m.Name)
					if err := cl.DestroyModel(cmd.Context(), m.Name, opts); err != nil {
						return fmt.Errorf("destroying model %q: %w", m.Name, err)
					}
				}
			}
			if err := install.Uninstall(cmd.Context(), kc); err != nil {
				return err
			}
			// Deleting returns at once; a reinstall before the CRDs are really gone would lose them.
			timeout := opts.Timeout
			if timeout == 0 {
				timeout = 10 * time.Minute
			}
			if err := install.WaitUninstalled(cmd.Context(), kc, timeout); err != nil {
				return err
			}
			fmt.Fprintln(cmd.OutOrStdout(), "jk uninstalled")
			return nil
		},
	}
	cmd.Flags().BoolVar(&force, "force", false, "skip the graceful removal of each model and delete its namespace at once")
	cmd.Flags().BoolVar(&destroyStorage, "destroy-storage", false, "delete the volumes of the models' applications (the default keeps them)")
	cmd.Flags().BoolVarP(&noPrompt, "no-prompt", "y", false, "do not ask for confirmation")
	cmd.Flags().StringVar(&timeout, "timeout", "", "how long to wait for each model's graceful removal (default 10m)")
	return cmd
}

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the jk version",
		Run:   func(cmd *cobra.Command, _ []string) { fmt.Fprintln(cmd.OutOrStdout(), version.Version) },
	}
}
