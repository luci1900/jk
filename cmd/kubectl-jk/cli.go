package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"golang.org/x/term"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	"github.com/luci1900/jk/pkg/sdk"
)

// cli carries what commands share: kubectl's connection flags and, for tests, a replacement for the SDK client.
type cli struct {
	flags *genericclioptions.ConfigFlags
	// newSDK, when set, replaces the client built from the kubeconfig.
	newSDK func(ctx context.Context, namespace string) (*sdk.Client, error)
	// stdin is where prompts read from (default os.Stdin).
	stdin io.Reader
	// runKubectl, when set, replaces running the kubectl binary (ssh and scp); newKube replaces the clientset (debug-log).
	runKubectl func(args []string) error
	newKube    func() (kubernetes.Interface, error)
	// newClient, when set, replaces the controller-runtime client install and uninstall use (it needs the CRD types).
	newClient func() (client.Client, error)
}

// installClient is the controller-runtime client for install and uninstall.
func (c *cli) installClient() (client.Client, error) {
	if c.newClient != nil {
		return c.newClient()
	}
	return newClient(c.flags)
}

// model is the namespace commands act on: -m or -n, else the kube context's namespace.
func (c *cli) model() (string, error) {
	ns, _, err := c.flags.ToRawKubeConfigLoader().Namespace()
	return ns, err
}

// sdk builds the SDK client for the current model.
func (c *cli) sdk(ctx context.Context) (*sdk.Client, error) {
	ns, err := c.model()
	if err != nil {
		return nil, err
	}
	if c.newSDK != nil {
		return c.newSDK(ctx, ns)
	}
	cfg, err := c.flags.ToRESTConfig()
	if err != nil {
		return nil, err
	}
	return sdk.New(cfg, ns)
}

// exitError makes a command fail with a given exit code and no message of its own (the output already said why).
type exitError struct{ code int }

func (e exitError) Error() string { return fmt.Sprintf("exit status %d", e.code) }

func exitCode(err error) int {
	var e exitError
	if errors.As(err, &e) {
		return e.code
	}
	return 1
}

// printErr prints an error message that is not tied to a failing command.
func printErr(cmd *cobra.Command, format string, args ...any) {
	fmt.Fprintf(cmd.ErrOrStderr(), format+"\n", args...)
}

// output formats.
const (
	formatTabular = "tabular"
	formatJSON    = "json"
	formatYAML    = "yaml"
)

// formatFlag adds juju's --format flag.
func formatFlag(cmd *cobra.Command, def string) *string {
	f := new(string)
	cmd.Flags().StringVar(f, "format", def, "output format: "+formatTabular+", "+formatJSON+" or "+formatYAML)
	return f
}

// printStructured writes v as JSON or YAML and reports whether format was one of those.
func printStructured(w io.Writer, format string, v any) (bool, error) {
	switch format {
	case formatJSON:
		b, err := json.MarshalIndent(v, "", "  ")
		if err != nil {
			return true, err
		}
		fmt.Fprintln(w, string(b))
		return true, nil
	case formatYAML:
		b, err := yaml.Marshal(v)
		if err != nil {
			return true, err
		}
		fmt.Fprint(w, string(b))
		return true, nil
	case formatTabular, "":
		return false, nil
	}
	return true, fmt.Errorf("unknown format %q: want %s, %s or %s", format, formatTabular, formatJSON, formatYAML)
}

func newTable(w io.Writer) *tabwriter.Writer { return tabwriter.NewWriter(w, 0, 1, 2, ' ', 0) }

// confirm asks a yes/no question on stderr; without a terminal and without --no-prompt it refuses.
func (c *cli) confirm(cmd *cobra.Command, question string) (bool, error) {
	in := c.stdin
	if in == nil {
		in = os.Stdin
	}
	if f, ok := in.(*os.File); ok && !term.IsTerminal(int(f.Fd())) {
		return false, fmt.Errorf("refusing to prompt without a terminal: pass --no-prompt to confirm")
	}
	fmt.Fprint(cmd.ErrOrStderr(), question+" [y/N]: ")
	line, _ := bufio.NewReader(in).ReadString('\n')
	switch strings.ToLower(strings.TrimSpace(line)) {
	case "y", "yes":
		return true, nil
	}
	return false, nil
}

// keyValues parses key=value arguments.
func keyValues(args []string) (map[string]string, error) {
	out := map[string]string{}
	for _, a := range args {
		k, v, ok := strings.Cut(a, "=")
		if !ok || k == "" {
			return nil, fmt.Errorf("invalid argument %q: want key=value", a)
		}
		out[k] = v
	}
	return out, nil
}

func (c *cli) commands() []*cobra.Command {
	var out []*cobra.Command
	out = append(out, c.modelCommands()...)
	out = append(out, c.charmCommands()...)
	out = append(out, c.appCommands()...)
	out = append(out, c.relationCommands()...)
	out = append(out, c.offerCommands()...)
	out = append(out, c.statusCommands()...)
	out = append(out, c.actionCommands()...)
	out = append(out, c.showCommands()...)
	out = append(out, c.accessCommands()...)
	return out
}
