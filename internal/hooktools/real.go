// SPDX-License-Identifier: AGPL-3.0-only
//
// Derived from juju (https://github.com/juju/juju), AGPL-3.0: the hook tools of
// internal/worker/uniter/runner/jujuc (argument handling, output formats and error messages).

package hooktools

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	yaml2 "go.yaml.in/yaml/v2"
)

// Status is a juju status.
type Status struct {
	State   string
	Message string
	Data    map[string]any
}

// Port is an opened port range. For icmp From and To are 0.
type Port struct {
	Protocol string
	From, To int
}

func (p Port) String() string {
	switch {
	case p.Protocol == "icmp":
		return "icmp"
	case p.From == p.To:
		return fmt.Sprintf("%d/%s", p.From, p.Protocol)
	}
	return fmt.Sprintf("%d-%d/%s", p.From, p.To, p.Protocol)
}

// Backend is what the real hook tools need from the agent. The agent implements it per hook execution,
// buffering writes so they are committed only if the hook succeeds (except statuses, which juju applies at once).
type Backend interface {
	UnitName() string
	// IsLeader reports leadership with juju's guarantee (the lease has at least 30s left).
	IsLeader() bool
	// Config is the effective config (options without a default and not set are absent) and ConfigKeys all declared option names.
	Config() map[string]any
	ConfigKeys() []string
	UnitStatus() (Status, bool)
	SetUnitStatus(Status) error
	// ApplicationStatus is only called by the leader.
	ApplicationStatus() (Status, bool)
	SetApplicationStatus(Status) error
	SetWorkloadVersion(string) error
	State() map[string]string
	SetState(map[string]string) error
	DeleteState(keys ...string) error
	// OpenedPorts are the committed ports (pending opens made in this hook are not included, as in juju).
	OpenedPorts() []Port
	OpenPort(Port) error
	ClosePort(Port) error
	// Address is the unit's IP.
	Address() string
	Log(level, message string)

	RelationBackend
	SecretBackend
	StorageBackend
	ClusterBackend
	ActionBackend
}

// Real implements the hook tools on a Backend.
type Real struct {
	B Backend
	// ReadFile reads --file arguments other than "-" (the agent shares a filesystem with the tool). Defaults to os.ReadFile.
	ReadFile func(string) ([]byte, error)
	// Now is the clock for --expire durations (time.Now by default).
	Now func() time.Time

	mu sync.Mutex
}

var _ Handler = (*Real)(nil)

// Call implements Handler.
func (r *Real) Call(req Request) Response {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch req.Name {
	case "is-leader":
		return r.isLeader(req)
	case "juju-log":
		return r.jujuLog(req)
	case "config-get":
		return r.configGet(req)
	case "status-get":
		return r.statusGet(req)
	case "status-set":
		return r.statusSet(req)
	case "application-version-set":
		return r.versionSet(req)
	case "state-get":
		return r.stateGet(req)
	case "state-set":
		return r.stateSet(req)
	case "state-delete":
		return r.stateDelete(req)
	case "open-port", "close-port":
		return r.port(req)
	case "opened-ports":
		return r.openedPorts(req)
	case "network-get":
		return r.networkGet(req)
	case "unit-get":
		return r.unitGet(req)
	case "goal-state":
		return r.goalState(req)
	case "relation-ids":
		return r.relationIDs(req)
	case "relation-list":
		return r.relationList(req)
	case "relation-get":
		return r.relationGet(req)
	case "relation-set":
		return r.relationSet(req)
	case "relation-model-get":
		return r.relationModelGet(req)
	case "secret-add":
		return r.secretAdd(req)
	case "secret-set":
		return r.secretSet(req)
	case "secret-get":
		return r.secretGet(req)
	case "secret-info-get":
		return r.secretInfoGet(req)
	case "secret-ids":
		return r.secretIDs(req)
	case "secret-remove":
		return r.secretRemove(req)
	case "secret-grant":
		return r.secretGrant(req)
	case "secret-revoke":
		return r.secretRevoke(req)
	case "storage-get":
		return r.storageGet(req)
	case "storage-list":
		return r.storageList(req)
	case "action-get":
		return r.actionGet(req)
	case "action-set":
		return r.actionSet(req)
	case "action-fail":
		return r.actionFail(req)
	case "action-log":
		return r.actionLog(req)
	case "juju-reboot":
		return failf("juju-reboot is not supported by jk (kubernetes sidecar charms are restarted by Kubernetes)")
	case "credential-get":
		return failf("cannot access cloud credentials: credential-get is not supported by jk")
	case "resource-get":
		return failf("resource-get is not supported by jk: charm resources are delivered as container images")
	case "storage-add":
		return failf("adding storage to a unit is not supported on k8s")
	}
	return failf("unknown hook tool %q", req.Name)
}

func parseReq(def flagDef, req Request) (cli, *Response) {
	c, err := parseCLI(def, req.Args)
	if err != nil {
		resp := failf("%v", err)
		return c, &resp
	}
	return c, nil
}

func (r *Real) isLeader(req Request) Response {
	c, bad := parseReq(withFormat(nil, nil), req)
	if bad != nil {
		return *bad
	}
	if len(c.pos) > 0 {
		return failf("unrecognized args: %q", c.pos)
	}
	return write(c, "smart", r.B.IsLeader())
}

func (r *Real) jujuLog(req Request) Response {
	c, bad := parseReq(flagDef{flags: map[string]bool{"debug": false, "l": true, "log-level": true, "format": true},
		aliases: map[string]string{"l": "log-level"}}, req)
	if bad != nil {
		return *bad
	}
	if len(c.pos) == 0 {
		return failf("no message specified")
	}
	level := "INFO"
	if c.boolean("debug") {
		level = "DEBUG"
	} else if l := c.str("log-level"); l != "" {
		switch up := strings.ToUpper(l); up {
		case "TRACE", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL":
			level = up
		default:
			r.B.Log("WARNING", fmt.Sprintf("Specified log level of %q is not valid", l))
		}
	}
	r.B.Log(level, strings.Join(c.pos, " "))
	var resp Response
	if c.has("format") {
		resp.Stderr = "--format flag deprecated for command \"juju-log\""
	}
	return resp
}

func (r *Real) configGet(req Request) Response {
	c, bad := parseReq(withFormat(map[string]bool{"all": false, "a": false}, map[string]string{"a": "all"}), req)
	if bad != nil {
		return *bad
	}
	key := ""
	if len(c.pos) > 0 {
		key = c.pos[0]
		if len(c.pos) > 1 {
			return failf("unrecognized args: %q", c.pos[1:])
		}
	}
	if key != "" && c.boolean("all") {
		return failf("cannot use argument --all together with key %q", key)
	}
	cfg := r.B.Config()
	if key != "" {
		// Missing keys are reported as null and are not an error.
		v, ok := cfg[key]
		if !ok {
			return write(c, "smart", nil)
		}
		return write(c, "smart", v)
	}
	out := make(map[string]any, len(cfg))
	for k, v := range cfg {
		out[k] = v
	}
	if c.boolean("all") {
		for _, k := range r.B.ConfigKeys() {
			if _, ok := out[k]; !ok {
				out[k] = nil
			}
		}
	}
	return write(c, "smart", out)
}

func statusDetails(s Status, includeData bool) map[string]any {
	d := map[string]any{"status": s.State}
	if includeData {
		data := map[string]any{}
		for k, v := range s.Data {
			data[k] = v
		}
		d["status-data"] = data
		d["message"] = s.Message
	}
	return d
}

func (r *Real) statusGet(req Request) Response {
	c, bad := parseReq(withFormat(map[string]bool{"include-data": false, "application": false}, nil), req)
	if bad != nil {
		return *bad
	}
	if len(c.pos) > 0 {
		return failf("unrecognized args: %q", c.pos)
	}
	includeData := c.boolean("include-data")
	smartPlain := !includeData && (c.str("format") == "" || c.str("format") == "smart")
	if c.boolean("application") {
		if !r.B.IsLeader() {
			return failf("finding application status: this unit is not the leader")
		}
		s, ok := r.B.ApplicationStatus()
		if !ok {
			s = Status{State: "unknown"}
		}
		if smartPlain {
			return write(c, "smart", s.State)
		}
		details := statusDetails(s, includeData)
		units := map[string]any{}
		if us, ok := r.B.UnitStatus(); ok {
			units[r.B.UnitName()] = statusDetails(us, includeData)
		}
		details["units"] = units
		return write(c, "smart", map[string]any{"application-status": details})
	}
	s, ok := r.B.UnitStatus()
	if !ok {
		s = Status{State: "unknown"}
	}
	if smartPlain {
		return write(c, "smart", s.State)
	}
	return write(c, "smart", statusDetails(s, includeData))
}

func (r *Real) statusSet(req Request) Response {
	c, bad := parseReq(flagDef{flags: map[string]bool{"application": false}}, req)
	if bad != nil {
		return *bad
	}
	if len(c.pos) < 1 {
		return failf("invalid args, require <status> [message]")
	}
	if len(c.pos) > 2 {
		return failf("unrecognized args: %q", c.pos[2:])
	}
	switch c.pos[0] {
	case "maintenance", "blocked", "waiting", "active":
	default:
		return failf("invalid status %q, expected one of [maintenance blocked waiting active]", c.pos[0])
	}
	st := Status{State: c.pos[0]}
	if len(c.pos) > 1 {
		st.Message = c.pos[1]
	}
	if c.boolean("application") {
		if !r.B.IsLeader() {
			return failf("this unit is not the leader")
		}
		if err := r.B.SetApplicationStatus(st); err != nil {
			return failf("%v", err)
		}
		return Response{}
	}
	if err := r.B.SetUnitStatus(st); err != nil {
		return failf("%v", err)
	}
	return Response{}
}

func (r *Real) versionSet(req Request) Response {
	c, bad := parseReq(flagDef{}, req)
	if bad != nil {
		return *bad
	}
	if len(c.pos) < 1 {
		return failf("no version specified")
	}
	if len(c.pos) > 1 {
		return failf("unrecognized args: %q", c.pos[1:])
	}
	if err := r.B.SetWorkloadVersion(c.pos[0]); err != nil {
		return failf("%v", err)
	}
	return Response{}
}

func (r *Real) stateGet(req Request) Response {
	c, bad := parseReq(withFormat(map[string]bool{"strict": false}, nil), req)
	if bad != nil {
		return *bad
	}
	key := ""
	if len(c.pos) > 0 {
		if key = c.pos[0]; key == "-" {
			key = ""
		}
		if len(c.pos) > 1 {
			return failf("unrecognized args: %q", c.pos[1:])
		}
	}
	st := r.B.State()
	if key == "" {
		if len(st) == 0 {
			// juju prints null for empty state in json, nothing in the other formats.
			return write(c, "smart", nilIfEmpty(st))
		}
		return write(c, "smart", st)
	}
	v, ok := st[key]
	if !ok {
		if c.boolean("strict") {
			return failf("%q not found", key)
		}
		// A missing key prints nothing and exits 0 (ops relies on it); json prints "".
		return write(c, "smart", "")
	}
	return write(c, "smart", v)
}

func nilIfEmpty(m map[string]string) any {
	if len(m) == 0 {
		return nil
	}
	return m
}

func (r *Real) readFile(path string, stdin string) ([]byte, error) {
	if path == "-" {
		return []byte(stdin), nil
	}
	if r.ReadFile != nil {
		return r.ReadFile(path)
	}
	return os.ReadFile(path)
}

// parseKeyValues parses `key=value` positionals; empty values are allowed.
func parseKeyValues(args []string) (map[string]string, error) {
	out := map[string]string{}
	for _, a := range args {
		k, v, ok := strings.Cut(a, "=")
		if !ok || k == "" {
			return nil, fmt.Errorf("expected \"key=value\", got %q", a)
		}
		out[k] = v
	}
	return out, nil
}

// readSettings reads a YAML (or JSON) mapping of keys to scalar values, as juju's gopkg.in/yaml.v2 decodes it into a
// map[string]string: keys such as "n" and "y" stay strings, and scalars keep their text.
func readSettings(b []byte) (map[string]string, error) {
	out := map[string]string{}
	if err := yaml2.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	if out == nil {
		out = map[string]string{}
	}
	return out, nil
}

func (r *Real) stateSet(req Request) Response {
	c, bad := parseReq(flagDef{flags: map[string]bool{"file": true}}, req)
	if bad != nil {
		return *bad
	}
	kvs, err := parseKeyValues(c.pos)
	if err != nil {
		return failf("%v", err)
	}
	if path := c.str("file"); path != "" {
		b, err := r.readFile(path, req.Stdin)
		if err != nil {
			return failf("%v", err)
		}
		fromFile, err := readSettings(b)
		if err != nil {
			return failf("%v", err)
		}
		for k, v := range kvs {
			fromFile[k] = v
		}
		kvs = fromFile
	}
	if err := r.B.SetState(kvs); err != nil {
		return failf("%v", err)
	}
	return Response{}
}

func (r *Real) stateDelete(req Request) Response {
	c, bad := parseReq(flagDef{}, req)
	if bad != nil {
		return *bad
	}
	if len(c.pos) < 1 {
		return failf("no key specified")
	}
	if len(c.pos) > 1 {
		return failf("unrecognized args: %q", c.pos[1:])
	}
	if err := r.B.DeleteState(c.pos...); err != nil {
		return failf("%v", err)
	}
	return Response{}
}

// ParsePort parses a port or port range as open-port does: "80", "80/tcp", "8000-8100/udp", "icmp".
func ParsePort(s string) (Port, error) {
	s = strings.ToLower(s)
	if s == "icmp" {
		return Port{Protocol: "icmp"}, nil
	}
	ports, proto, _ := strings.Cut(s, "/")
	if proto == "" {
		proto = "tcp"
	}
	switch proto {
	case "tcp", "udp", "icmp":
	default:
		return Port{}, fmt.Errorf("invalid protocol %q, expected \"tcp\", \"udp\", or \"icmp\"", proto)
	}
	if proto == "icmp" {
		return Port{Protocol: "icmp"}, nil
	}
	fromS, toS, isRange := strings.Cut(ports, "-")
	from, err := strconv.Atoi(fromS)
	if err != nil {
		return Port{}, fmt.Errorf("invalid port %q", ports)
	}
	to := from
	if isRange {
		if to, err = strconv.Atoi(toS); err != nil {
			return Port{}, fmt.Errorf("invalid port range %q", ports)
		}
	}
	if from < 1 || to > 65535 || from > to {
		return Port{}, fmt.Errorf("port range bounds must be between 1 and 65535, and the first bound must not exceed the last: %q", ports)
	}
	return Port{Protocol: proto, From: from, To: to}, nil
}

func (r *Real) port(req Request) Response {
	c, bad := parseReq(flagDef{flags: map[string]bool{"format": true, "endpoints": true}}, req)
	if bad != nil {
		return *bad
	}
	if len(c.pos) == 0 {
		return failf("no port or range specified")
	}
	if len(c.pos) > 1 {
		return failf("unrecognized args: %q", c.pos[1:])
	}
	p, err := ParsePort(c.pos[0])
	if err != nil {
		return failf("%v", err)
	}
	if req.Name == "open-port" {
		err = r.B.OpenPort(p)
	} else {
		err = r.B.ClosePort(p)
	}
	if err != nil {
		return failf("%v", err)
	}
	var resp Response
	if c.has("format") {
		resp.Stderr = fmt.Sprintf("--format flag deprecated for command %q", req.Name)
	}
	return resp
}

// SortPorts orders ports as juju does: by protocol, then range.
func SortPorts(ports []Port) {
	sort.Slice(ports, func(i, j int) bool {
		a, b := ports[i], ports[j]
		if a.Protocol != b.Protocol {
			return a.Protocol < b.Protocol
		}
		if a.From != b.From {
			return a.From < b.From
		}
		return a.To < b.To
	})
}

func (r *Real) openedPorts(req Request) Response {
	c, bad := parseReq(withFormat(map[string]bool{"endpoints": false}, nil), req)
	if bad != nil {
		return *bad
	}
	if len(c.pos) > 0 {
		return failf("unrecognized args: %q", c.pos)
	}
	ports := append([]Port(nil), r.B.OpenedPorts()...)
	SortPorts(ports)
	out := make([]string, 0, len(ports))
	for _, p := range ports {
		s := p.String()
		if c.boolean("endpoints") {
			s += " (*)" // jk opens ports on all endpoints
		}
		out = append(out, s)
	}
	return write(c, "smart", out)
}

func (r *Real) unitGet(req Request) Response {
	c, bad := parseReq(withFormat(nil, nil), req)
	if bad != nil {
		return *bad
	}
	if len(c.pos) == 0 {
		return failf("no setting specified")
	}
	if c.pos[0] != "private-address" && c.pos[0] != "public-address" {
		return failf("unknown setting %q", c.pos[0])
	}
	if len(c.pos) > 1 {
		return failf("unrecognized args: %q", c.pos[1:])
	}
	return write(c, "smart", r.B.Address())
}
