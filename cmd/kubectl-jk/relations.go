package main

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/luci1900/jk/pkg/sdk"
)

func (c *cli) relationCommands() []*cobra.Command {
	var alias string
	integrate := &cobra.Command{
		Use:     "integrate <application>[:<endpoint>] <application>[:<endpoint>] | <application>[:<endpoint>] <model>.<offer>[:<endpoint>]",
		Aliases: []string{"relate"},
		Short:   "Relate two applications",
		Args:    cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			cl, err := c.sdk(cmd.Context())
			if err != nil {
				return err
			}
			rel, err := cl.Integrate(cmd.Context(), args[0], args[1], sdk.IntegrateOptions{Alias: alias})
			if err != nil {
				return err
			}
			a, b := rel.Spec.Endpoints[0], rel.Spec.Endpoints[1]
			other := b.Application
			if b.Namespace != rel.Namespace {
				other = sdk.OfferURL(b.Namespace, rel.Spec.Offer)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Integrated %s:%s with %s:%s\n", a.Application, a.Endpoint, other, b.Endpoint)
			return nil
		},
	}
	integrate.Flags().StringVar(&alias, "alias", "", "for an offer of another model: the name its application has in this model (default: the offer's name)")
	remove := &cobra.Command{
		Use:     "remove-relation <application>[:<endpoint>] <application>[:<endpoint>]",
		Aliases: []string{"remove-integration"},
		Short:   "Remove the relation between two applications",
		Args:    cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			cl, err := c.sdk(cmd.Context())
			if err != nil {
				return err
			}
			if err := cl.RemoveRelation(cmd.Context(), args[0], args[1]); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "will remove the relation between %s and %s\n", args[0], args[1])
			return nil
		},
	}
	return []*cobra.Command{integrate, remove}
}
