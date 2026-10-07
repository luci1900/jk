package main

import (
	"fmt"
	"sort"
	"strings"

	"github.com/spf13/cobra"
)

func (c *cli) charmCommands() []*cobra.Command {
	format := new(string)
	find := &cobra.Command{
		Use:   "find [query]",
		Short: "Search Charmhub for charms",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cl, err := c.sdk(cmd.Context())
			if err != nil {
				return err
			}
			q := ""
			if len(args) > 0 {
				q = args[0]
			}
			res, err := cl.Find(cmd.Context(), q)
			if err != nil {
				return err
			}
			if done, err := printStructured(cmd.OutOrStdout(), *format, res); done {
				return err
			}
			tw := newTable(cmd.OutOrStdout())
			fmt.Fprintln(tw, "Name\tVersion\tPublisher\tSummary")
			for _, r := range res {
				fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", r.Name, r.Version, r.Publisher, firstLine(r.Summary))
			}
			return tw.Flush()
		},
	}
	format = formatFlag(find, formatTabular)

	iformat := new(string)
	info := &cobra.Command{
		Use:   "info <charm>",
		Short: "Show the channels and revisions of a charm in Charmhub",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cl, err := c.sdk(cmd.Context())
			if err != nil {
				return err
			}
			in, err := cl.Info(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			if done, err := printStructured(cmd.OutOrStdout(), *iformat, in); done {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "name: %s\n", in.Name)
			if in.Default != nil {
				fmt.Fprintf(cmd.OutOrStdout(), "default channel: %s (revision %d)\n", in.Default.Channel, in.Default.Revision)
			}
			rows := append(in.Channels[:0:0], in.Channels...)
			sort.SliceStable(rows, func(i, j int) bool { return rows[i].Channel < rows[j].Channel })
			tw := newTable(cmd.OutOrStdout())
			fmt.Fprintln(tw, "Channel\tBase\tRevision\tVersion")
			for _, r := range rows {
				fmt.Fprintf(tw, "%s\t%s\t%d\t%s\n", r.Channel, r.Base, r.Revision, r.Version)
			}
			return tw.Flush()
		},
	}
	iformat = formatFlag(info, formatTabular)
	return []*cobra.Command{find, info}
}

func firstLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	return s
}
