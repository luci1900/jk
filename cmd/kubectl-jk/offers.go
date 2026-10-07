package main

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/luci1900/jk/pkg/sdk"
)

func (c *cli) offerCommands() []*cobra.Command {
	var allow []string
	offer := &cobra.Command{
		Use:   "offer <application>:<endpoint>[,<endpoint>...] [offer-name]",
		Short: "Offer endpoints of an application to other models",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			app, eps, ok := strings.Cut(args[0], ":")
			if !ok || app == "" || eps == "" {
				return fmt.Errorf("invalid offer %q: want <application>:<endpoint>[,<endpoint>...]", args[0])
			}
			o := sdk.OfferOptions{Application: app, Endpoints: strings.Split(eps, ",")}
			if len(args) == 2 {
				o.Name = args[1]
			}
			if cmd.Flags().Changed("allow") {
				o.AllowedModels = allow
			}
			cl, err := c.sdk(cmd.Context())
			if err != nil {
				return err
			}
			res, err := cl.Offer(cmd.Context(), o)
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Application %q endpoints [%s] available at %q\n", app, strings.Join(res.Spec.Endpoints, ", "), sdk.OfferURL(cl.Namespace, res.Name))
			return nil
		},
	}
	offer.Flags().StringSliceVar(&allow, "allow", nil, "models (namespaces) that may consume the offer, `*` for all (default: the model config offer-allowed-models)")

	format := new(string)
	offers := &cobra.Command{
		Use:     "offers",
		Aliases: []string{"list-offers"},
		Short:   "List the offers of the model",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cl, err := c.sdk(cmd.Context())
			if err != nil {
				return err
			}
			list, err := cl.Offers(cmd.Context())
			if err != nil {
				return err
			}
			if done, err := printStructured(cmd.OutOrStdout(), *format, list); done {
				return err
			}
			tw := newTable(cmd.OutOrStdout())
			fmt.Fprintln(tw, "Offer\tApplication\tEndpoints\tConnected\tAllowed models")
			for _, o := range list {
				allowed := strings.Join(o.Spec.AllowedModels, ",")
				if allowed == "" {
					allowed = "-"
				}
				fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\n", sdk.OfferURL(cl.Namespace, o.Name), o.Spec.Application, strings.Join(o.Spec.Endpoints, ","), len(o.Status.Connections), allowed)
			}
			return tw.Flush()
		},
	}
	format = formatFlag(offers, formatTabular)

	sformat := new(string)
	show := &cobra.Command{
		Use:   "show-offer <model>.<offer>",
		Short: "Show an offer: its endpoints and who is connected",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cl, err := c.sdk(cmd.Context())
			if err != nil {
				return err
			}
			o, err := cl.ShowOffer(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			view := map[string]any{
				"url": sdk.OfferURL(o.Namespace, o.Name), "application": o.Spec.Application, "endpoints": o.Status.Endpoints,
				"allowed-models": o.Spec.AllowedModels, "connections": o.Status.Connections,
			}
			if *sformat == formatTabular {
				*sformat = formatYAML
			}
			_, err = printStructured(cmd.OutOrStdout(), *sformat, view)
			return err
		},
	}
	sformat = formatFlag(show, formatYAML)

	var force bool
	remove := &cobra.Command{
		Use:   "remove-offer <offer>",
		Short: "Remove an offer (with --force, together with the relations that use it)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cl, err := c.sdk(cmd.Context())
			if err != nil {
				return err
			}
			if err := cl.RemoveOffer(cmd.Context(), args[0], force, 0); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Removed offer %s\n", args[0])
			return nil
		},
	}
	remove.Flags().BoolVar(&force, "force", false, "remove the offer although other models are connected to it")
	return []*cobra.Command{offer, offers, show, remove}
}
