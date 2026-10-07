// Package charmhub is a small client for the Charmhub store API: it resolves a charm name, channel, base and
// architecture to a revision with its download URL, sha256, metadata, config and actions, and downloads the
// charm. Only the parts jk needs are implemented. The HTTP client is injectable for tests.
package charmhub

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	// DefaultURL is the public Charmhub API.
	DefaultURL = "https://api.charmhub.io"
	// DefaultChannel is used when a request names none.
	DefaultChannel = "latest/stable"

	// unknown is the placeholder base that makes Charmhub answer with the channel's default bases.
	unknown = "NA"

	maxResponse = 64 << 20
)

// Client talks to Charmhub. The zero value uses the public API and http.DefaultClient.
type Client struct {
	// BaseURL is the API root, DefaultURL when empty.
	BaseURL string
	// HTTPClient is used for all requests, http.DefaultClient when nil.
	HTTPClient *http.Client
	// UserAgent is sent with every request when set.
	UserAgent string
}

// Base is an operating system base and architecture, e.g. ubuntu 22.04 amd64.
type Base struct {
	Name         string `json:"name"`
	Channel      string `json:"channel"`
	Architecture string `json:"architecture"`
}

// String is "name@channel" (the form used in Application specs), or "" if the base is unknown.
func (b Base) String() string {
	if b.Name == "" || b.Channel == "" {
		return ""
	}
	return b.Name + "@" + b.Channel
}

// ParseBase parses "name@channel" (e.g. ubuntu@22.04); the empty string gives the zero Base.
func ParseBase(s string) (Base, error) {
	if s == "" {
		return Base{}, nil
	}
	n, c, ok := strings.Cut(s, "@")
	if !ok || n == "" || c == "" {
		return Base{}, fmt.Errorf("invalid base %q: want name@channel, e.g. ubuntu@22.04", s)
	}
	return Base{Name: n, Channel: c}, nil
}

// Request selects one charm revision.
type Request struct {
	Name string
	// Channel defaults to DefaultChannel. Ignored when Revision is set.
	Channel string
	// Revision pins a revision (revisions are per architecture).
	Revision *int
	// Base: Architecture is required. When Name and Channel are empty, the channel's default base is looked up.
	Base Base
}

// Resource is a resource of a charm revision.
type Resource struct {
	Name        string
	Type        string
	Description string
	Revision    int
	// DownloadURL is the Charmhub URL of the resource blob. For oci-image resources it needs credentials.
	DownloadURL string
}

// Charm is a resolved charm revision.
type Charm struct {
	ID       string
	Name     string
	Revision int
	Version  string
	Summary  string
	// Channel is the channel the revision was found in (empty for revision pins).
	Channel    string
	ReleasedAt time.Time
	// Base is the base and architecture the revision was resolved for.
	Base Base
	// Bases are all the bases of the revision.
	Bases []Base
	// DownloadURL and SHA256 (hex) identify the charm file; Size is in bytes.
	DownloadURL string
	SHA256      string
	Size        int64
	// MetadataYAML, ConfigYAML and ActionsYAML are the contents of the charm's files (config and actions may be empty).
	MetadataYAML string
	ConfigYAML   string
	ActionsYAML  string
	Resources    []Resource
}

// Error is an error reported by the Charmhub API.
type Error struct {
	Code    string
	Message string
	// Releases lists the channels and bases that do have a revision ("revision-not-found" only).
	Releases []Release
	// DefaultBases lists the bases of the requested channel ("invalid-charm-base" only).
	DefaultBases []Base
}

// Release is a channel and base a charm is released to.
type Release struct {
	Channel string
	Base    Base
}

func (e *Error) Error() string {
	if e.Code == "" {
		return "charmhub: " + e.Message
	}
	return fmt.Sprintf("charmhub: %s: %s", e.Code, e.Message)
}

// Is reports whether err is a Charmhub API error with the given code.
func Is(err error, code string) bool {
	var e *Error
	return errors.As(err, &e) && e.Code == code
}

// Error codes returned by Charmhub that callers care about.
const (
	CodeNameNotFound     = "name-not-found"
	CodeRevisionNotFound = "revision-not-found"
	CodeInvalidBase      = "invalid-charm-base"
	CodeMissingBase      = "missing-charm-base"
)

// IsPermanent reports whether retrying the same request cannot help: the charm, channel, revision or base
// does not exist. Transport failures and server errors are not permanent.
func IsPermanent(err error) bool {
	var e *Error
	if !errors.As(err, &e) {
		return false
	}
	switch e.Code {
	case CodeNameNotFound, CodeRevisionNotFound, CodeInvalidBase, CodeMissingBase, "bad-argument":
		return true
	}
	return false
}

// NormalizeChannel turns the empty channel into DefaultChannel and a bare risk ("stable") into
// "latest/<risk>", as juju does; Charmhub does not accept a bare risk on its own.
func NormalizeChannel(ch string) string {
	switch ch {
	case "":
		return DefaultChannel
	case "stable", "candidate", "beta", "edge":
		return "latest/" + ch
	}
	return ch
}

func (c *Client) baseURL() string {
	if c.BaseURL != "" {
		return strings.TrimRight(c.BaseURL, "/")
	}
	return DefaultURL
}

func (c *Client) http() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return http.DefaultClient
}

func (c *Client) do(ctx context.Context, method, u string, body any, out any) error {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, rd)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.UserAgent != "" {
		req.Header.Set("User-Agent", c.UserAgent)
	}
	resp, err := c.http().Do(req)
	if err != nil {
		return fmt.Errorf("charmhub: %w", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponse))
	if err != nil {
		return fmt.Errorf("charmhub: reading response: %w", err)
	}
	if resp.StatusCode == http.StatusNotFound {
		return &Error{Code: CodeNameNotFound, Message: "not found in the store (" + u + ")"}
	}
	if resp.StatusCode != http.StatusOK {
		// Charmhub reports API errors in the body of 4xx responses too.
		var e apiErrors
		if json.Unmarshal(data, &e) == nil && len(e.ErrorList) > 0 {
			return e.ErrorList[0].toError()
		}
		return fmt.Errorf("charmhub: %s %s: HTTP %d: %s", method, u, resp.StatusCode, truncate(string(data), 200))
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("charmhub: decoding response: %w", err)
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Extra   struct {
		Releases []struct {
			Base    Base   `json:"base"`
			Channel string `json:"channel"`
		} `json:"releases"`
		DefaultBases []Base `json:"default-bases"`
	} `json:"extra"`
}

type apiErrors struct {
	ErrorList []apiError `json:"error-list"`
}

func (e apiError) toError() *Error {
	out := &Error{Code: e.Code, Message: e.Message, DefaultBases: e.Extra.DefaultBases}
	for _, r := range e.Extra.Releases {
		out.Releases = append(out.Releases, Release{Channel: r.Channel, Base: r.Base})
	}
	return out
}

type refreshAction struct {
	Action      string  `json:"action"`
	InstanceKey string  `json:"instance-key"`
	Name        string  `json:"name"`
	Channel     *string `json:"channel,omitempty"`
	Revision    *int    `json:"revision,omitempty"`
	Base        Base    `json:"base"`
}

type refreshRequest struct {
	Context []struct{}      `json:"context"`
	Actions []refreshAction `json:"actions"`
	Fields  []string        `json:"fields"`
}

type refreshResponse struct {
	ErrorList []apiError `json:"error-list"`
	Results   []struct {
		Charm            *refreshCharm `json:"charm"`
		EffectiveChannel string        `json:"effective-channel"`
		Error            *apiError     `json:"error"`
		ID               string        `json:"id"`
		Name             string        `json:"name"`
		ReleasedAt       string        `json:"released-at"`
	} `json:"results"`
}

type refreshCharm struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Revision int    `json:"revision"`
	Version  string `json:"version"`
	Summary  string `json:"summary"`
	Bases    []Base `json:"bases"`
	Download struct {
		URL  string `json:"url"`
		Hash string `json:"hash-sha-256"`
		Size int64  `json:"size"`
	} `json:"download"`
	MetadataYAML string `json:"metadata-yaml"`
	ConfigYAML   string `json:"config-yaml"`
	ActionsYAML  string `json:"actions-yaml"`
	Resources    []struct {
		Name        string `json:"name"`
		Type        string `json:"type"`
		Description string `json:"description"`
		Revision    int    `json:"revision"`
		Download    struct {
			URL string `json:"url"`
		} `json:"download"`
	} `json:"resources"`
}

var refreshFields = []string{
	"actions-yaml", "bases", "config-yaml", "download", "id", "metadata-yaml", "name",
	"resources", "revision", "summary", "type", "version",
}

// Resolve finds the charm revision for the request. Without a base name and channel it asks Charmhub for the
// channel's default bases for the architecture and takes the newest. Errors from the store are *Error.
func (c *Client) Resolve(ctx context.Context, r Request) (*Charm, error) {
	if r.Name == "" {
		return nil, errors.New("charmhub: charm name is required")
	}
	if r.Base.Architecture == "" {
		return nil, errors.New("charmhub: architecture is required")
	}
	base := r.Base
	if base.Name == "" || base.Channel == "" {
		base.Name, base.Channel = unknown, unknown
	}
	res, err := c.refresh(ctx, r, base)
	if r.Base.Name != "" && r.Base.Channel != "" {
		return res, err
	}
	if err == nil {
		// A revision pin resolves without a base; Charmhub tells us which one it found.
		res.Base = Base{Architecture: r.Base.Architecture}
		for _, b := range res.Bases {
			if b.Architecture == r.Base.Architecture {
				res.Base = b
				break
			}
		}
		return res, nil
	}
	var e *Error
	if !errors.As(err, &e) || e.Code != CodeInvalidBase {
		return nil, err
	}
	picked, ok := newestBase(e.DefaultBases, r.Base.Architecture)
	if !ok {
		return nil, fmt.Errorf("charmhub: no base of %s in channel %q is available for %s", r.Name, NormalizeChannel(r.Channel), r.Base.Architecture)
	}
	return c.refresh(ctx, r, picked)
}

// newestBase picks the base with the highest channel (by version, so 24.04 beats 22.04) for the architecture.
func newestBase(bases []Base, arch string) (Base, bool) {
	var cands []Base
	for _, b := range bases {
		if b.Architecture == arch || b.Architecture == "" {
			cands = append(cands, Base{Name: b.Name, Channel: b.Channel, Architecture: arch})
		}
	}
	if len(cands) == 0 {
		return Base{}, false
	}
	sort.SliceStable(cands, func(i, j int) bool { return versionLess(cands[i].Channel, cands[j].Channel) })
	return cands[len(cands)-1], true
}

func versionLess(a, b string) bool {
	as, bs := strings.Split(a, "."), strings.Split(b, ".")
	for i := 0; i < len(as) && i < len(bs); i++ {
		x, ex := strconv.Atoi(as[i])
		y, ey := strconv.Atoi(bs[i])
		if ex != nil || ey != nil {
			if as[i] != bs[i] {
				return as[i] < bs[i]
			}
			continue
		}
		if x != y {
			return x < y
		}
	}
	return len(as) < len(bs)
}

func (c *Client) refresh(ctx context.Context, r Request, base Base) (*Charm, error) {
	act := refreshAction{Action: "install", InstanceKey: "jk", Name: r.Name, Revision: r.Revision, Base: base}
	if r.Revision == nil {
		ch := NormalizeChannel(r.Channel)
		act.Channel = &ch
	}
	var resp refreshResponse
	err := c.do(ctx, http.MethodPost, c.baseURL()+"/v2/charms/refresh", refreshRequest{
		Context: []struct{}{}, Actions: []refreshAction{act}, Fields: refreshFields,
	}, &resp)
	if err != nil {
		return nil, err
	}
	if len(resp.ErrorList) > 0 {
		return nil, resp.ErrorList[0].toError()
	}
	if len(resp.Results) != 1 {
		return nil, fmt.Errorf("charmhub: refresh returned %d results, want 1", len(resp.Results))
	}
	res := resp.Results[0]
	if res.Error != nil {
		return nil, res.Error.toError()
	}
	ch := res.Charm
	if ch == nil {
		return nil, errors.New("charmhub: refresh returned no charm")
	}
	out := &Charm{
		ID: ch.ID, Name: ch.Name, Revision: ch.Revision, Version: ch.Version, Summary: ch.Summary,
		Channel: res.EffectiveChannel, Bases: ch.Bases, Base: base,
		DownloadURL: ch.Download.URL, SHA256: ch.Download.Hash, Size: ch.Download.Size,
		MetadataYAML: ch.MetadataYAML, ConfigYAML: ch.ConfigYAML, ActionsYAML: ch.ActionsYAML,
	}
	if out.ID == "" {
		out.ID = res.ID
	}
	if out.Name == "" {
		out.Name = res.Name
	}
	if t, err := time.Parse(time.RFC3339, res.ReleasedAt); err == nil {
		out.ReleasedAt = t
	}
	for _, rs := range ch.Resources {
		out.Resources = append(out.Resources, Resource{Name: rs.Name, Type: rs.Type, Description: rs.Description, Revision: rs.Revision, DownloadURL: rs.Download.URL})
	}
	if out.DownloadURL == "" || out.SHA256 == "" {
		return nil, fmt.Errorf("charmhub: %s revision %d has no download information", r.Name, ch.Revision)
	}
	return out, nil
}

// ChannelRelease is one entry of a charm's channel map.
type ChannelRelease struct {
	// Channel is the full channel name, e.g. "14/stable".
	Channel    string
	Track      string
	Risk       string
	Base       Base
	Revision   int
	Version    string
	ReleasedAt time.Time
}

// Info is what the info endpoint knows about a charm.
type Info struct {
	ID   string
	Name string
	// Channels lists every channel and base with its revision.
	Channels []ChannelRelease
	// Default is the default release (the newest stable one), when there is one.
	Default *ChannelRelease
}

type infoRelease struct {
	Channel struct {
		Name       string `json:"name"`
		Track      string `json:"track"`
		Risk       string `json:"risk"`
		Base       Base   `json:"base"`
		ReleasedAt string `json:"released-at"`
	} `json:"channel"`
	Revision struct {
		Revision int    `json:"revision"`
		Version  string `json:"version"`
	} `json:"revision"`
}

func (r infoRelease) release() ChannelRelease {
	out := ChannelRelease{Channel: r.Channel.Name, Track: r.Channel.Track, Risk: r.Channel.Risk, Base: r.Channel.Base, Revision: r.Revision.Revision, Version: r.Revision.Version}
	if t, err := time.Parse(time.RFC3339, r.Channel.ReleasedAt); err == nil {
		out.ReleasedAt = t
	}
	return out
}

// Info fetches a charm's channel map.
func (c *Client) Info(ctx context.Context, name string) (*Info, error) {
	if name == "" {
		return nil, errors.New("charmhub: charm name is required")
	}
	q := url.Values{"fields": {"channel-map.revision.revision,channel-map.revision.version,channel-map.revision.bases,channel-map.channel.released-at,default-release.revision.revision"}}
	var resp struct {
		ID             string        `json:"id"`
		Name           string        `json:"name"`
		ChannelMap     []infoRelease `json:"channel-map"`
		DefaultRelease *infoRelease  `json:"default-release"`
	}
	if err := c.do(ctx, http.MethodGet, c.baseURL()+"/v2/charms/info/"+url.PathEscape(name)+"?"+q.Encode(), nil, &resp); err != nil {
		return nil, err
	}
	out := &Info{ID: resp.ID, Name: resp.Name}
	for _, r := range resp.ChannelMap {
		out.Channels = append(out.Channels, r.release())
	}
	if resp.DefaultRelease != nil {
		d := resp.DefaultRelease.release()
		out.Default = &d
	}
	return out, nil
}

// FindResult is one charm found by Find.
type FindResult struct {
	Name      string
	Summary   string
	Publisher string
	// Version is the workload version of the default release, when Charmhub reports one.
	Version string
}

// Find searches the store for charms matching the query (empty lists the most relevant ones).
func (c *Client) Find(ctx context.Context, query string) ([]FindResult, error) {
	q := url.Values{"fields": {"result.summary,result.publisher.display-name,default-release.revision.version"}}
	if query != "" {
		q.Set("q", query)
	}
	var resp struct {
		Results []struct {
			Name   string `json:"name"`
			Type   string `json:"type"`
			Result struct {
				Summary   string `json:"summary"`
				Publisher struct {
					DisplayName string `json:"display-name"`
				} `json:"publisher"`
			} `json:"result"`
			DefaultRelease struct {
				Revision struct {
					Version string `json:"version"`
				} `json:"revision"`
			} `json:"default-release"`
		} `json:"results"`
	}
	if err := c.do(ctx, http.MethodGet, c.baseURL()+"/v2/charms/find?"+q.Encode(), nil, &resp); err != nil {
		return nil, err
	}
	var out []FindResult
	for _, r := range resp.Results {
		if r.Type != "" && r.Type != "charm" {
			continue
		}
		out = append(out, FindResult{Name: r.Name, Summary: r.Result.Summary, Publisher: r.Result.Publisher.DisplayName, Version: r.DefaultRelease.Revision.Version})
	}
	return out, nil
}
