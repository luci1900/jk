package charmhub

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// allowedHost reports whether u may be downloaded from: the API host itself or charmhub.io.
func (c *Client) allowedHost(u *url.URL) bool {
	if b, err := url.Parse(c.baseURL()); err == nil && b.Host == u.Host {
		return true
	}
	h := u.Hostname()
	return h == "charmhub.io" || strings.HasSuffix(h, ".charmhub.io")
}

// Download fetches the charm file at u into w and verifies its sha256 (hex, case-insensitive) when given.
// The URL must be on the Charmhub API host or charmhub.io, since it can come from an Application spec.
// On a mismatch the bytes have already been written to w; the caller must discard them.
func (c *Client) Download(ctx context.Context, u, wantSHA256 string, w io.Writer) (written int64, err error) {
	pu, err := url.Parse(u)
	if err != nil || (pu.Scheme != "https" && pu.Scheme != "http") || pu.Host == "" {
		return 0, fmt.Errorf("charmhub: invalid download URL %q", u)
	}
	if !c.allowedHost(pu) {
		return 0, fmt.Errorf("charmhub: refusing to download from %q: not a Charmhub host", pu.Host)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return 0, err
	}
	if c.UserAgent != "" {
		req.Header.Set("User-Agent", c.UserAgent)
	}
	resp, err := c.http().Do(req)
	if err != nil {
		return 0, fmt.Errorf("charmhub: downloading charm: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("charmhub: downloading charm: HTTP %d", resp.StatusCode)
	}
	h := sha256.New()
	written, err = io.Copy(io.MultiWriter(w, h), resp.Body)
	if err != nil {
		return written, fmt.Errorf("charmhub: downloading charm: %w", err)
	}
	if got := hex.EncodeToString(h.Sum(nil)); wantSHA256 != "" && !strings.EqualFold(got, wantSHA256) {
		return written, fmt.Errorf("charmhub: charm checksum mismatch: got sha256 %s, want %s", got, wantSHA256)
	}
	return written, nil
}
