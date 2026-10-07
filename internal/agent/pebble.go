package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"k8s.io/utils/clock"

	"github.com/luci1900/jk/internal/agent/resolver"
)

// PebblePollInterval is how often each container's Pebble is polled. juju polls every 5s; this is a cheap local socket call and pebble-ready waits on it.
const PebblePollInterval = time.Second

// PebbleProber asks a container's Pebble for its boot ID. It returns an error while Pebble does not answer.
type PebbleProber interface {
	BootID(ctx context.Context, container string) (string, error)
}

// SocketPebble probes Pebble over the per-container sockets at <Dir>/<container>/pebble.socket
// (the charm container sees each workload's socket at /charm/containers/<name>/pebble.socket).
type SocketPebble struct {
	Dir string
}

// BootID implements PebbleProber using GET /v1/system-info.
func (p SocketPebble) BootID(ctx context.Context, container string) (string, error) {
	socket := filepath.Join(p.Dir, container, "pebble.socket")
	hc := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", socket)
			},
			DisableKeepAlives: true,
		},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://pebble/v1/system-info", nil)
	if err != nil {
		return "", err
	}
	resp, err := hc.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("pebble system-info: %s", resp.Status)
	}
	var body struct {
		Result struct {
			BootID string `json:"boot-id"`
		} `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", err
	}
	if body.Result.BootID == "" {
		return "", fmt.Errorf("pebble system-info has no boot-id")
	}
	return body.Result.BootID, nil
}

// PebblePoller polls every container and remembers the last boot ID each answered with.
type PebblePoller struct {
	Prober     PebbleProber
	Containers []string
	Clock      clock.Clock
	Interval   time.Duration

	mu    sync.Mutex
	ready map[string]string
}

// Snapshot returns the readiness of each container (empty BootID while Pebble does not answer).
func (p *PebblePoller) Snapshot() []resolver.Pebble {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]resolver.Pebble, 0, len(p.Containers))
	for _, c := range p.Containers {
		out = append(out, resolver.Pebble{Container: c, BootID: p.ready[c]})
	}
	return out
}

// Poll probes all containers once and reports whether anything changed.
func (p *PebblePoller) Poll(ctx context.Context) (changed bool) {
	for _, c := range p.Containers {
		id, err := p.Prober.BootID(ctx, c)
		if err != nil {
			id = "" // not up (yet)
		}
		p.mu.Lock()
		if p.ready == nil {
			p.ready = map[string]string{}
		}
		if p.ready[c] != id {
			p.ready[c] = id
			changed = true
		}
		p.mu.Unlock()
	}
	return changed
}

// Run polls until ctx ends, calling notify when readiness changes.
func (p *PebblePoller) Run(ctx context.Context, notify func()) {
	interval := p.Interval
	if interval <= 0 {
		interval = PebblePollInterval
	}
	t := p.Clock.NewTimer(interval)
	defer t.Stop()
	for {
		if p.Poll(ctx) {
			notify()
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C():
			t.Reset(interval)
		}
	}
}
