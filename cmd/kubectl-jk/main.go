// Command kubectl-jk is the jk CLI, a kubectl plugin (`kubectl jk ...`).
package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericclioptions"
)

func main() {
	if err := newRoot().Execute(); err != nil {
		var e exitError
		if !errors.As(err, &e) {
			label := "ERROR"
			if _, off := os.LookupEnv("NO_COLOR"); !off && isTerminal(os.Stderr) {
				label = red + label + reset
			}
			fmt.Fprintln(os.Stderr, label, err)
		}
		os.Exit(exitCode(err))
	}
}

func newRoot() *cobra.Command {
	return newRootWith(&cli{flags: genericclioptions.NewConfigFlags(true)})
}

// newRootWith builds the command tree around a cli (tests pass one that talks to a fake cluster).
func newRootWith(cli *cli) *cobra.Command {
	flags := cli.flags
	root := &cobra.Command{
		Use:           "jk",
		Short:         "Run juju charms on Kubernetes",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	// -m (juju's --model) picks the namespace. kubectl's --namespace stays as a hidden long alias, without -n,
	// which is juju's --num-units.
	ns := flags.Namespace
	flags.Namespace = nil
	flags.AddFlags(root.PersistentFlags())
	flags.Namespace = ns
	root.PersistentFlags().StringVarP(flags.Namespace, "model", "m", "", "the model (the namespace); default: the kube context's namespace")
	root.PersistentFlags().StringVar(flags.Namespace, "namespace", "", "the model (kubectl's name for it)")
	_ = root.PersistentFlags().MarkHidden("namespace")
	root.AddCommand(newInstallCmd(flags), cli.uninstallCmd(), newVersionCmd())
	for _, c := range cli.commands() {
		root.AddCommand(c)
	}
	root.AddCommand(unsupportedCommands()...)
	cli.addCompletions(root)
	return root
}
