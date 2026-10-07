package main

import (
	"sort"
	"strings"

	"github.com/spf13/cobra"
)

// completer returns the candidates for the word being completed.
type completer func(cmd *cobra.Command, toComplete string) []string

// completions look up names in the cluster. A lookup that fails offers nothing, since a completion must not print errors.
func (c *cli) complete(f completer, max int) func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
	return func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		if max > 0 && len(args) >= max {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}
		var out []string
		for _, s := range f(cmd, toComplete) {
			if strings.HasPrefix(s, toComplete) {
				out = append(out, s)
			}
		}
		return out, cobra.ShellCompDirectiveNoFileComp
	}
}

func (c *cli) applicationNames(cmd *cobra.Command, _ string) []string {
	cl, err := c.sdk(cmd.Context())
	if err != nil {
		return nil
	}
	st, err := cl.Status(cmd.Context())
	if err != nil {
		return nil
	}
	return sortedKeys(st.Applications)
}

// unitNames are the units of the model and, as `run` accepts them, "<app>/leader".
func (c *cli) unitNames(cmd *cobra.Command, _ string) []string {
	cl, err := c.sdk(cmd.Context())
	if err != nil {
		return nil
	}
	st, err := cl.Status(cmd.Context())
	if err != nil {
		return nil
	}
	var out []string
	for _, a := range sortedKeys(st.Applications) {
		units := make([]string, 0, len(st.Applications[a].Units))
		for u := range st.Applications[a].Units {
			units = append(units, u)
		}
		sort.Slice(units, func(i, j int) bool { return unitOrder(units[i]) < unitOrder(units[j]) })
		out = append(out, units...)
	}
	return out
}

func (c *cli) unitOrLeaderNames(cmd *cobra.Command, s string) []string {
	out := c.unitNames(cmd, s)
	seen := map[string]bool{}
	for _, u := range out {
		app, _, _ := strings.Cut(u, "/")
		seen[app] = true
	}
	for _, app := range sortedKeys(seen) {
		out = append(out, app+"/leader")
	}
	return out
}

func (c *cli) modelNames(cmd *cobra.Command, _ string) []string {
	cl, err := c.sdk(cmd.Context())
	if err != nil {
		return nil
	}
	models, err := cl.Models(cmd.Context())
	if err != nil {
		return nil
	}
	var out []string
	for _, m := range models {
		out = append(out, m.Name)
	}
	return out
}

func (c *cli) appsOrUnits(cmd *cobra.Command, s string) []string {
	return append(c.applicationNames(cmd, s), c.unitNames(cmd, s)...)
}

// addCompletions completes model, application and unit names, for kubectl's `__complete` (see cmd/kubectl_complete-jk).
func (c *cli) addCompletions(root *cobra.Command) {
	find := func(name string) *cobra.Command {
		if cmd, _, err := root.Find([]string{name}); err == nil && cmd.Name() == name {
			return cmd
		}
		return nil
	}
	// args completes a command's positional arguments, up to max of them (0 for any number).
	args := func(f completer, max int, names ...string) {
		for _, n := range names {
			if cmd := find(n); cmd != nil {
				cmd.ValidArgsFunction = c.complete(f, max)
			}
		}
	}
	flag := func(cmd *cobra.Command, name string, f completer) {
		if cmd != nil {
			_ = cmd.RegisterFlagCompletionFunc(name, c.complete(f, 0))
		}
	}
	args(c.applicationNames, 1, "refresh", "trust", "scale-application", "add-unit", "remove-unit", "actions", "constraints", "set-constraints", "config")
	args(c.applicationNames, 0, "remove-application", "show-application")
	args(c.unitNames, 0, "show-unit", "resolved")
	args(c.unitNames, 1, "ssh")
	args(c.unitOrLeaderNames, 0, "run")
	args(c.modelNames, 1, "switch", "destroy-model")
	args(c.appsOrUnits, 0, "status")
	flag(root, "model", c.modelNames)
	flag(find("exec"), "unit", c.unitNames)
	flag(find("exec"), "application", c.applicationNames)
	flag(find("debug-log"), "include", c.appsOrUnits)
}
