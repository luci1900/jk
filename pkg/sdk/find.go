package sdk

import (
	"context"

	"github.com/luci1900/jk/pkg/charmhub"
)

// Find searches Charmhub.
func (c *Client) Find(ctx context.Context, query string) ([]charmhub.FindResult, error) {
	return c.Hub.Find(ctx, query)
}

// Info describes a charm in Charmhub: its channels and revisions.
func (c *Client) Info(ctx context.Context, name string) (*charmhub.Info, error) {
	return c.Hub.Info(ctx, name)
}
