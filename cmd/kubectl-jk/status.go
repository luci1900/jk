// SPDX-License-Identifier: AGPL-3.0-only
//
// Derived from juju (https://github.com/juju/juju), AGPL-3.0: cmd/juju/status/output_tabular.go (the tabular
// layout).

package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/luci1900/jk/pkg/sdk"
)

func (c *cli) statusCommands() []*cobra.Command {
	var relations, watch, color, noColor bool
	format := new(string)
	status := &cobra.Command{
		Use:   "status [application|unit]...",
		Short: "Show the model's applications, units and relations",
		Long:  "Show the model's applications, units and relations. Arguments are names or globs, such as 'pg*' or 'pg/0', and limit what is shown.",
		RunE: func(cmd *cobra.Command, patterns []string) error {
			if color && noColor {
				return fmt.Errorf("--color and --no-color cannot be used together")
			}
			useColor := wantColor(cmd.OutOrStdout(), color, noColor)
			if *format == "smart" {
				*format = formatTabular
			}
			cl, err := c.sdk(cmd.Context())
			if err != nil {
				return err
			}
			show := func(ctx context.Context) error {
				st, err := cl.Status(ctx)
				if err != nil {
					return err
				}
				if len(patterns) > 0 {
					if st, err = filterStatus(st, patterns); err != nil {
						return err
					}
				}
				if done, err := printStructured(cmd.OutOrStdout(), *format, st); done {
					return err
				}
				return writeStatus(cmd.OutOrStdout(), st, relations, useColor)
			}
			if !watch {
				return show(cmd.Context())
			}
			clear := isTerminal(cmd.OutOrStdout())
			for {
				if clear {
					fmt.Fprint(cmd.OutOrStdout(), "\033[H\033[2J")
				}
				if err := show(cmd.Context()); err != nil {
					fmt.Fprintln(cmd.ErrOrStderr(), "ERROR", err)
				}
				select {
				case <-cmd.Context().Done():
					return nil
				case <-time.After(time.Second):
				}
			}
		},
	}
	status.Flags().BoolVar(&relations, "relations", false, "show relations too")
	status.Flags().BoolVar(&relations, "integrations", false, "same as --relations")
	status.Flags().BoolVar(&color, "color", false, "colour the output even when it is not a terminal")
	status.Flags().BoolVar(&noColor, "no-color", false, "do not colour the output")
	status.Flags().BoolVar(&watch, "watch", false, "refresh every second")
	format = formatFlag(status, formatTabular)
	status.Flags().Lookup("format").Usage += " (smart is tabular)"

	var noRetry bool
	resolved := &cobra.Command{
		Use:     "resolved <unit>...",
		Aliases: []string{"resolve"},
		Short:   "Retry the failed hook of a unit now (or skip it with --no-retry)",
		Args:    cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cl, err := c.sdk(cmd.Context())
			if err != nil {
				return err
			}
			for _, u := range args {
				if err := cl.Resolved(cmd.Context(), u, noRetry); err != nil {
					return err
				}
			}
			return nil
		},
	}
	resolved.Flags().BoolVar(&noRetry, "no-retry", false, "skip the failed hook instead of retrying it")
	return []*cobra.Command{status, resolved}
}

func isTerminal(w io.Writer) bool {
	f, ok := w.(*os.File)
	return ok && term.IsTerminal(int(f.Fd()))
}

func sinceString(s sdk.StatusInfo) string {
	if s.Since == nil {
		return ""
	}
	return s.Since.UTC().Format("02 Jan 2006 15:04:05Z")
}

// writeStatus prints the model as juju's tabular status does, without the controller and cloud columns.
func writeStatus(w io.Writer, st *sdk.Status, relations, color bool) error {
	var buf bytes.Buffer
	if err := renderStatus(&buf, st, relations); err != nil {
		return err
	}
	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	for i := range lines {
		lines[i] = strings.TrimRight(lines[i], " ")
	}
	if color {
		colorize(lines)
	}
	for _, line := range lines {
		fmt.Fprintln(w, line)
	}
	return nil
}

func renderStatus(w io.Writer, st *sdk.Status, relations bool) error {
	tw := newTable(w)
	fmt.Fprintln(tw, "Model\tVersion\tTimestamp")
	fmt.Fprintf(tw, "%s\t%s\t%s\n", st.Model.Name, st.Model.Version, st.Model.Timestamp.UTC().Format("15:04:05Z"))
	fmt.Fprintln(tw)
	if len(st.Applications) == 0 {
		fmt.Fprintln(tw, "Model is empty.")
		return tw.Flush()
	}
	fmt.Fprintln(tw, "App\tVersion\tStatus\tScale\tCharm\tChannel\tRev\tAddress\tExposed\tMessage")
	apps := sortedKeys(st.Applications)
	for _, name := range apps {
		a := st.Applications[name]
		ready := 0
		for _, u := range a.Units {
			if u.WorkloadStatus.Current == "active" {
				ready++
			}
		}
		channel, rev := a.CharmChannel, fmt.Sprint(a.CharmRev)
		if a.CharmOrigin == "local" {
			channel, rev = "", "-"
		}
		exposed := "no"
		if a.Exposed {
			exposed = "yes"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\n", name, a.Version, a.Status.Current, a.Scale, a.Charm, channel, rev, a.Address, exposed, a.Status.Message)
	}
	fmt.Fprintln(tw)
	if len(st.RemoteApplications) > 0 {
		fmt.Fprintln(tw, "SAAS\tStatus\tStore\tURL")
		for _, name := range sortedKeys(st.RemoteApplications) {
			ra := st.RemoteApplications[name]
			fmt.Fprintf(tw, "%s\t%s\tlocal\t%s\n", name, ra.Status, ra.Offer)
		}
		fmt.Fprintln(tw)
	}
	fmt.Fprintln(tw, "Unit\tWorkload\tAgent\tAddress\tPorts\tMessage")
	for _, name := range apps {
		a := st.Applications[name]
		units := make([]string, 0, len(a.Units))
		for u := range a.Units {
			units = append(units, u)
		}
		sort.Slice(units, func(i, j int) bool { return unitOrder(units[i]) < unitOrder(units[j]) })
		for _, u := range units {
			us := a.Units[u]
			id := u
			if us.Leader {
				id += "*"
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n", id, us.WorkloadStatus.Current, us.AgentStatus.Current, us.Address, strings.Join(us.OpenedPorts, ","), us.WorkloadStatus.Message)
		}
	}
	if len(st.Offers) > 0 {
		fmt.Fprintln(tw)
		fmt.Fprintln(tw, "Offer\tApplication\tEndpoints\tConnected\tMessage")
		for _, name := range sortedKeys(st.Offers) {
			o := st.Offers[name]
			var eps []string
			for _, e := range o.Endpoints {
				eps = append(eps, e.Name)
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\n", name, o.Application, strings.Join(eps, ","), len(o.Connections), o.Message)
		}
	}
	if relations && len(st.Relations) > 0 {
		fmt.Fprintln(tw)
		fmt.Fprintln(tw, "Integration provider\tRequirer\tInterface\tType\tMessage")
		for _, r := range st.Relations {
			msg := ""
			if r.Status != "joined" {
				msg = r.Status
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", r.Provider, r.Requirer, r.Interface, r.Type, msg)
		}
	}
	return tw.Flush()
}

// unitOrder sorts "app/2" before "app/10".
func unitOrder(u string) string {
	app, n, _ := strings.Cut(u, "/")
	return fmt.Sprintf("%s/%08s", app, n)
}

// filterStatus keeps the applications and units that match one of the globs (an application name, or a unit such as
// "pg/0"). An application matched by one of its units shows only those units, as in juju.
func filterStatus(st *sdk.Status, patterns []string) (*sdk.Status, error) {
	for _, p := range patterns {
		if _, err := path.Match(p, ""); err != nil {
			return nil, fmt.Errorf("invalid pattern %q", p)
		}
	}
	match := func(name string) bool {
		for _, p := range patterns {
			if ok, _ := path.Match(p, name); ok {
				return true
			}
		}
		return false
	}
	out := *st
	out.Applications = map[string]sdk.ApplicationStatus{}
	for name, a := range st.Applications {
		if match(name) {
			out.Applications[name] = a
			continue
		}
		units := map[string]sdk.UnitStatus{}
		for u, us := range a.Units {
			if match(u) {
				units[u] = us
			}
		}
		if len(units) > 0 {
			a.Units = units
			out.Applications[name] = a
		}
	}
	if len(out.Applications) == 0 {
		return nil, fmt.Errorf("nothing matches %s", strings.Join(patterns, ", "))
	}
	out.Relations = nil
	for _, r := range st.Relations {
		if relatesTo(r.Provider, out.Applications) || relatesTo(r.Requirer, out.Applications) {
			out.Relations = append(out.Relations, r)
		}
	}
	out.Offers = nil
	for name, o := range st.Offers {
		if _, ok := out.Applications[o.Application]; ok {
			if out.Offers == nil {
				out.Offers = map[string]sdk.OfferStatus{}
			}
			out.Offers[name] = o
		}
	}
	return &out, nil
}

// relatesTo is whether an endpoint, "app:endpoint", belongs to one of the applications.
func relatesTo(endpoint string, apps map[string]sdk.ApplicationStatus) bool {
	app, _, _ := strings.Cut(endpoint, ":")
	_, ok := apps[app]
	return ok
}
