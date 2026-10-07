package main

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"sigs.k8s.io/yaml"

	"github.com/luci1900/jk/api/v1alpha1"
	"github.com/luci1900/jk/pkg/sdk"
)

func (c *cli) actionCommands() []*cobra.Command {
	return []*cobra.Command{c.actionsCmd(), c.runCmd(), c.execCmd(), c.operationsCmd(), c.showOperationCmd(), c.showTaskCmd()}
}

func (c *cli) actionsCmd() *cobra.Command {
	format := new(string)
	cmd := &cobra.Command{
		Use:     "actions <application>",
		Aliases: []string{"list-actions"},
		Short:   "List the actions of an application's charm",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cl, err := c.sdk(cmd.Context())
			if err != nil {
				return err
			}
			acts, err := cl.Actions(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			if done, err := printStructured(cmd.OutOrStdout(), *format, acts); done {
				return err
			}
			tw := newTable(cmd.OutOrStdout())
			fmt.Fprintln(tw, "Action\tDescription")
			for _, n := range sortedKeys(acts) {
				fmt.Fprintf(tw, "%s\t%s\n", n, firstLine(acts[n]))
			}
			return tw.Flush()
		},
	}
	format = formatFlag(cmd, formatTabular)
	return cmd
}

// runArgs splits `run`'s arguments: units (they contain a slash), the action name, then key=value parameters.
func runArgs(args []string) (units []string, action string, params []string, err error) {
	i := 0
	for i < len(args) && strings.Contains(args[i], "/") && !strings.Contains(args[i], "=") {
		units = append(units, args[i])
		i++
	}
	if len(units) == 0 {
		return nil, "", nil, fmt.Errorf("no unit specified: want <application>/<number> or <application>/leader")
	}
	if i >= len(args) {
		return nil, "", nil, fmt.Errorf("no action specified")
	}
	return units, args[i], args[i+1:], nil
}

func (c *cli) runCmd() *cobra.Command {
	var background bool
	var wait string
	format := new(string)
	cmd := &cobra.Command{
		Use:   "run <unit>... <action> [key=value ...]",
		Short: "Run an action on units and show its results",
		Args:  cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			units, action, rawParams, err := runArgs(args)
			if err != nil {
				return err
			}
			params, err := sdk.ParseParams(rawParams)
			if err != nil {
				return err
			}
			cl, err := c.sdk(cmd.Context())
			if err != nil {
				return err
			}
			timeout, err := optDuration(wait)
			if err != nil {
				return err
			}
			return c.runAndShow(cmd, cl, sdk.RunOptions{Units: units, Action: action, Params: params}, background, timeout, *format)
		},
	}
	cmd.Flags().BoolVar(&background, "background", false, "start the action and return its operation id")
	cmd.Flags().StringVar(&wait, "wait", "", "how long to wait for the action (default: until it finishes)")
	format = formatFlag(cmd, formatTabular)
	return cmd
}

func (c *cli) execCmd() *cobra.Command {
	var units, apps []string
	var timeout string
	var background bool
	format := new(string)
	cmd := &cobra.Command{
		Use:   "exec [--unit <unit>]... [--application <app>]... -- <command>...",
		Short: "Run a command on units, in the charm container's hook environment",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(units) == 0 && len(apps) == 0 {
				return fmt.Errorf("specify --unit or --application")
			}
			d, err := optDuration(timeout)
			if err != nil {
				return err
			}
			cl, err := c.sdk(cmd.Context())
			if err != nil {
				return err
			}
			params := map[string]any{"command": strings.Join(args, " ")}
			return c.runAndShow(cmd, cl, sdk.RunOptions{Units: units, Applications: apps, Action: v1alpha1.ActionExec, Params: params, Timeout: d}, background, d, *format)
		},
	}
	cmd.Flags().StringArrayVar(&units, "unit", nil, "unit to run on (repeatable)")
	cmd.Flags().StringArrayVar(&apps, "application", nil, "run on every unit of the application (repeatable)")
	cmd.Flags().StringVar(&timeout, "timeout", "", "stop the command after this long")
	cmd.Flags().BoolVar(&background, "background", false, "start the command and return its operation id")
	format = formatFlag(cmd, formatTabular)
	return cmd
}

func optDuration(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	return parseDuration(s)
}

// runAndShow starts an action and prints what juju prints: the operation, then each task's results. It fails with
// exit status 1 when a task did not complete.
func (c *cli) runAndShow(cmd *cobra.Command, cl *sdk.Client, o sdk.RunOptions, background bool, timeout time.Duration, format string) error {
	out := cmd.OutOrStdout()
	op, err := cl.StartAction(cmd.Context(), o)
	if err != nil {
		return err
	}
	structured := format != formatTabular && format != ""
	if !structured {
		fmt.Fprintf(out, "Running operation %d with %d task%s\n", op.ID, len(op.Tasks), plural(len(op.Tasks)))
		for _, t := range op.Tasks {
			fmt.Fprintf(out, "  - task %d on unit-%s\n", t.ID, strings.ReplaceAll(t.Unit, "/", "-"))
		}
		fmt.Fprintln(out)
	}
	if background {
		if !structured {
			fmt.Fprintf(out, "Check operation status with 'jk show-operation %d'\nCheck task status with 'jk show-task %d'\n", op.ID, op.Tasks[0].ID)
		}
		return nil
	}
	if !structured {
		ids := make([]string, 0, len(op.Tasks))
		for _, t := range op.Tasks {
			ids = append(ids, strconv.FormatInt(t.ID, 10))
		}
		fmt.Fprintf(out, "Waiting for task%s %s...\n", plural(len(ids)), strings.Join(ids, ", "))
	}
	done, err := cl.WaitOperation(cmd.Context(), op.ID, timeout)
	if err != nil {
		return err
	}
	if structured {
		if _, err := printStructured(out, format, taskMap(done)); err != nil {
			return err
		}
	} else {
		writeTasks(out, cmd.ErrOrStderr(), done.Tasks)
	}
	for _, t := range done.Tasks {
		if t.Status != "completed" || exitStatus(t) != 0 {
			return exitError{1}
		}
	}
	return nil
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

func exitStatus(t *sdk.Task) int {
	switch v := t.Results["return-code"].(type) {
	case float64:
		return int(v)
	case int64:
		return int(v)
	case int:
		return v
	}
	return 0
}

// taskMap is the structured form of an operation's tasks, keyed as juju keys them.
func taskMap(op *sdk.Operation) map[string]any {
	out := map[string]any{}
	for _, t := range op.Tasks {
		out[strconv.FormatInt(t.ID, 10)] = taskView(t)
	}
	return out
}

func taskView(t *sdk.Task) map[string]any {
	v := map[string]any{"id": strconv.FormatInt(t.ID, 10), "unit": t.Unit, "action": t.Action, "status": t.Status}
	if t.Message != "" {
		v["message"] = t.Message
	}
	if len(t.Results) > 0 {
		v["results"] = t.Results
	}
	if len(t.Log) > 0 {
		v["log"] = t.Log
	}
	return v
}

// writeTasks prints results: a command's stdout and stderr as they are, the other results as YAML. With several
// tasks each is headed by its unit.
func writeTasks(out, errOut io.Writer, tasks []*sdk.Task) {
	for i, t := range tasks {
		if len(tasks) > 1 {
			if i > 0 {
				fmt.Fprintln(out)
			}
			fmt.Fprintf(out, "%s:\n", t.Unit)
		}
		for _, l := range t.Log {
			fmt.Fprintf(errOut, "%s %s\n", l.Timestamp.UTC().Format("15:04:05"), l.Message)
		}
		rest := map[string]any{}
		for k, v := range t.Results {
			switch k {
			case "stdout", "stderr", "return-code", "Code", "Stdout", "Stderr":
			default:
				rest[k] = v
			}
		}
		if s, _ := t.Results["stdout"].(string); s != "" {
			fmt.Fprint(out, strings.TrimRight(s, "\n")+"\n")
		}
		if s, _ := t.Results["stderr"].(string); s != "" {
			fmt.Fprint(errOut, strings.TrimRight(s, "\n")+"\n")
		}
		if len(rest) > 0 {
			b, _ := yaml.Marshal(rest)
			fmt.Fprint(out, string(b))
		}
		if t.Status != "completed" {
			msg := t.Message
			if msg == "" {
				msg = t.Status
			}
			fmt.Fprintf(errOut, "ERROR task %d %s: %s\n", t.ID, t.Status, msg)
		} else if rc := exitStatus(t); rc != 0 {
			fmt.Fprintf(errOut, "ERROR task %d: command exited with status %d\n", t.ID, rc)
		}
	}
}

func (c *cli) operationsCmd() *cobra.Command {
	format := new(string)
	cmd := &cobra.Command{
		Use:     "operations",
		Aliases: []string{"list-operations"},
		Short:   "List operations (what `run` and `exec` start)",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cl, err := c.sdk(cmd.Context())
			if err != nil {
				return err
			}
			ops, err := cl.Operations(cmd.Context())
			if err != nil {
				return err
			}
			if done, err := printStructured(cmd.OutOrStdout(), *format, ops); done {
				return err
			}
			tw := newTable(cmd.OutOrStdout())
			fmt.Fprintln(tw, "ID\tStatus\tStarted\tFinished\tTask IDs\tSummary")
			for _, op := range ops {
				var ids []string
				for _, t := range op.Tasks { // already in id order
					ids = append(ids, strconv.FormatInt(t.ID, 10))
				}
				fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\t%s\n", op.ID, op.Status, timeCol(op.Started), timeCol(op.Completed), strings.Join(ids, ","), op.Summary)
			}
			return tw.Flush()
		},
	}
	format = formatFlag(cmd, formatTabular)
	return cmd
}

func timeCol(t *time.Time) string {
	if t == nil {
		return "-"
	}
	return t.UTC().Format("2006-01-02T15:04:05Z")
}

func (c *cli) showOperationCmd() *cobra.Command {
	format := new(string)
	cmd := &cobra.Command{
		Use:   "show-operation <id>",
		Short: "Show an operation and its tasks",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := strconv.ParseInt(args[0], 10, 64)
			if err != nil {
				return fmt.Errorf("invalid operation id %q", args[0])
			}
			cl, err := c.sdk(cmd.Context())
			if err != nil {
				return err
			}
			op, err := cl.Operation(cmd.Context(), id)
			if err != nil {
				return err
			}
			view := map[string]any{"id": strconv.FormatInt(op.ID, 10), "summary": op.Summary, "status": op.Status, "enqueued": op.Enqueued.UTC(), "tasks": taskMap(op)}
			if *format == formatTabular {
				*format = formatYAML
			}
			_, err = printStructured(cmd.OutOrStdout(), *format, view)
			return err
		},
	}
	format = formatFlag(cmd, formatYAML)
	return cmd
}

func (c *cli) showTaskCmd() *cobra.Command {
	format := new(string)
	cmd := &cobra.Command{
		Use:   "show-task <id>",
		Short: "Show a task: its status, log and results",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := strconv.ParseInt(args[0], 10, 64)
			if err != nil {
				return fmt.Errorf("invalid task id %q", args[0])
			}
			cl, err := c.sdk(cmd.Context())
			if err != nil {
				return err
			}
			t, err := cl.Task(cmd.Context(), id)
			if err != nil {
				return err
			}
			view := taskView(t)
			view["operation"] = strconv.FormatInt(t.Operation, 10)
			view["enqueued"] = t.Enqueued.UTC()
			if *format == formatTabular {
				*format = formatYAML
			}
			_, err = printStructured(cmd.OutOrStdout(), *format, view)
			return err
		},
	}
	format = formatFlag(cmd, formatYAML)
	return cmd
}
