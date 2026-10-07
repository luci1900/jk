// SPDX-License-Identifier: AGPL-3.0-only

package hooktools

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

type fakeBackend struct {
	*fakeCluster
	fakeActions
	unit      string
	leader    bool
	cfg       map[string]any
	keys      []string
	status    *Status
	appStatus *Status
	version   string
	state     map[string]string
	ports     []Port
	pending   []string
	addr      string
	logs      []string
	err       error
}

func newFake() *fakeBackend {
	return &fakeBackend{fakeCluster: newFakeCluster(), unit: "app/0", cfg: map[string]any{"greeting": "hi", "retries": int64(3), "ratio": 0.5, "verbose": true},
		keys: []string{"greeting", "retries", "ratio", "verbose", "nodefault"}, state: map[string]string{}, addr: "10.1.2.3"}
}

func (f *fakeBackend) UnitName() string                  { return f.unit }
func (f *fakeBackend) IsLeader() bool                    { return f.leader }
func (f *fakeBackend) Config() map[string]any            { return f.cfg }
func (f *fakeBackend) ConfigKeys() []string              { return f.keys }
func (f *fakeBackend) UnitStatus() (Status, bool)        { return deref(f.status) }
func (f *fakeBackend) ApplicationStatus() (Status, bool) { return deref(f.appStatus) }
func deref(s *Status) (Status, bool) {
	if s == nil {
		return Status{}, false
	}
	return *s, true
}
func (f *fakeBackend) SetUnitStatus(s Status) error { f.status = &s; return f.err }
func (f *fakeBackend) SetApplicationStatus(s Status) error {
	f.appStatus = &s
	return f.err
}
func (f *fakeBackend) SetWorkloadVersion(v string) error { f.version = v; return f.err }
func (f *fakeBackend) State() map[string]string          { return f.state }
func (f *fakeBackend) SetState(kv map[string]string) error {
	for k, v := range kv {
		f.state[k] = v
	}
	return f.err
}
func (f *fakeBackend) DeleteState(keys ...string) error {
	for _, k := range keys {
		delete(f.state, k)
	}
	return f.err
}
func (f *fakeBackend) OpenedPorts() []Port { return f.ports }
func (f *fakeBackend) OpenPort(p Port) error {
	f.pending = append(f.pending, "open "+p.String())
	return f.err
}
func (f *fakeBackend) ClosePort(p Port) error {
	f.pending = append(f.pending, "close "+p.String())
	return f.err
}
func (f *fakeBackend) Address() string { return f.addr }
func (f *fakeBackend) NetworkInfo() NetworkInfo {
	if f.addr == "" {
		return NetworkInfo{}
	}
	return NetworkInfo{Interface: "eth0", MAC: "aa:bb:cc:dd:ee:ff", Address: f.addr, CIDR: f.addr + "/32"}
}
func (f *fakeBackend) Log(level, msg string) {
	f.logs = append(f.logs, level+": "+msg)
}

func rcall(r *Real, stdin, name string, args ...string) Response {
	return r.Call(Request{Name: name, Args: args, Stdin: stdin})
}

func newReal(t *testing.T) (*Real, *fakeBackend) {
	t.Helper()
	f := newFake()
	return &Real{B: f, Now: func() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) }}, f
}

func TestIsLeader(t *testing.T) {
	r, f := newReal(t)
	tests := []struct {
		leader bool
		args   []string
		want   string
	}{
		{true, nil, "True\n"}, {false, nil, "False\n"},
		{true, []string{"--format=json"}, "true\n"}, {false, []string{"--format", "json"}, "false\n"},
		{true, []string{"--format=yaml"}, "true\n"},
	}
	for _, tt := range tests {
		f.leader = tt.leader
		if got := rcall(r, "", "is-leader", tt.args...); got.Code != 0 || got.Stdout != tt.want {
			t.Errorf("is-leader %v (leader=%v) = %+v, want %q", tt.args, tt.leader, got, tt.want)
		}
	}
	if got := rcall(r, "", "is-leader", "extra"); got.Code == 0 {
		t.Error("extra args accepted")
	}
	if got := rcall(r, "", "is-leader", "--bogus"); got.Code == 0 || !strings.Contains(got.Stderr, "flag provided but not defined: --bogus") {
		t.Errorf("unknown flag: %+v", got)
	}
	if got := rcall(r, "", "is-leader", "--format=xml"); got.Code == 0 {
		t.Errorf("bad format accepted: %+v", got)
	}
	if got := rcall(r, "", "is-leader", "--format"); got.Code == 0 {
		t.Errorf("flag without value accepted: %+v", got)
	}
}

func TestConfigGet(t *testing.T) {
	r, _ := newReal(t)
	tests := []struct {
		name string
		args []string
		want string
		code int
	}{
		{"string", []string{"greeting"}, "hi\n", 0},
		{"int", []string{"retries"}, "3\n", 0},
		{"float", []string{"ratio"}, "0.5\n", 0},
		{"bool smart is Python style", []string{"verbose"}, "True\n", 0},
		{"json string", []string{"greeting", "--format=json"}, "\"hi\"\n", 0},
		{"json int", []string{"retries", "--format", "json"}, "3\n", 0},
		{"missing key is null in json", []string{"nope", "--format=json"}, "null\n", 0},
		{"missing key is empty in smart", []string{"nope"}, "", 0},
		{"unset option with default-less schema", []string{"nodefault", "--format=json"}, "null\n", 0},
		{"all keys json", []string{"--format=json"}, `{"greeting":"hi","ratio":0.5,"retries":3,"verbose":true}` + "\n", 0},
		{"all keys yaml", []string{"--format=yaml"}, "greeting: hi\nratio: 0.5\nretries: 3\nverbose: true\n", 0},
		{"--all includes nulls", []string{"--all", "--format=json"}, `{"greeting":"hi","nodefault":null,"ratio":0.5,"retries":3,"verbose":true}` + "\n", 0},
		{"-a", []string{"-a", "--format=json"}, `{"greeting":"hi","nodefault":null,"ratio":0.5,"retries":3,"verbose":true}` + "\n", 0},
		{"flags after the key", []string{"greeting", "--all"}, "", 1},
		{"too many args", []string{"a", "b"}, "", 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := rcall(r, "", "config-get", tt.args...)
			if got.Code != tt.code || (tt.code == 0 && got.Stdout != tt.want) {
				t.Fatalf("got %+v, want code %d stdout %q", got, tt.code, tt.want)
			}
		})
	}
}

func TestStatusSetAndGet(t *testing.T) {
	r, f := newReal(t)
	// Unknown before anything is set.
	if got := rcall(r, "", "status-get"); got.Stdout != "unknown\n" {
		t.Fatalf("%+v", got)
	}
	for _, bad := range [][]string{{}, {"nonsense"}, {"error", "x"}, {"unknown"}, {"active", "m", "extra"}, {"--bogus", "active"}} {
		if got := rcall(r, "", "status-set", bad...); got.Code == 0 {
			t.Errorf("status-set %v accepted", bad)
		}
	}
	if got := rcall(r, "", "status-set", "maintenance", "installing"); got.Code != 0 || got.Stdout != "" || !reflect.DeepEqual(*f.status, Status{State: "maintenance", Message: "installing"}) {
		t.Fatalf("%+v %+v", got, f.status)
	}
	if got := rcall(r, "", "status-set", "active"); got.Code != 0 || f.status.Message != "" {
		t.Fatalf("%+v %+v", got, f.status)
	}
	rcall(r, "", "status-set", "blocked", "need db")
	tests := []struct {
		args []string
		want string
	}{
		{nil, "blocked\n"},
		{[]string{"--format=json"}, `{"status":"blocked"}` + "\n"},
		{[]string{"--include-data", "--format=json"}, `{"message":"need db","status":"blocked","status-data":{}}` + "\n"},
		{[]string{"--include-data=true", "--format=yaml"}, "message: need db\nstatus: blocked\nstatus-data: {}\n"},
		{[]string{"--application=false", "--include-data", "--format=json"}, `{"message":"need db","status":"blocked","status-data":{}}` + "\n"},
		{[]string{"--application=False", "--include-data", "--format=json"}, `{"message":"need db","status":"blocked","status-data":{}}` + "\n"},
	}
	for _, tt := range tests {
		if got := rcall(r, "", "status-get", tt.args...); got.Code != 0 || got.Stdout != tt.want {
			t.Errorf("status-get %v = %+v, want %q", tt.args, got, tt.want)
		}
	}
	if got := rcall(r, "", "status-get", "extra"); got.Code == 0 {
		t.Error("extra args accepted")
	}
	f.err = errors.New("api down")
	if got := rcall(r, "", "status-set", "active"); got.Code == 0 || !strings.Contains(got.Stderr, "api down") {
		t.Errorf("backend error hidden: %+v", got)
	}
}

func TestApplicationStatus(t *testing.T) {
	r, f := newReal(t)
	// Non-leaders cannot set or read the application status.
	if got := rcall(r, "", "status-set", "--application", "active", "x"); got.Code != 1 || !strings.Contains(got.Stderr, "this unit is not the leader") {
		t.Fatalf("%+v", got)
	}
	if f.appStatus != nil {
		t.Fatal("non-leader wrote the application status")
	}
	if got := rcall(r, "", "status-get", "--application"); got.Code == 0 {
		t.Fatalf("non-leader read application status: %+v", got)
	}
	f.leader = true
	if got := rcall(r, "", "status-get", "--application"); got.Stdout != "unknown\n" {
		t.Fatalf("%+v", got)
	}
	if got := rcall(r, "", "status-set", "--application", "waiting", "for peers"); got.Code != 0 {
		t.Fatalf("%+v", got)
	}
	if !reflect.DeepEqual(*f.appStatus, Status{State: "waiting", Message: "for peers"}) || f.status != nil {
		t.Fatalf("app %+v unit %+v", f.appStatus, f.status)
	}
	if got := rcall(r, "", "status-get", "--application"); got.Stdout != "waiting\n" {
		t.Fatalf("%+v", got)
	}
	f.status = &Status{State: "active", Message: "ok"}
	got := rcall(r, "", "status-get", "--application", "--include-data", "--format=json")
	var parsed map[string]map[string]any
	if err := json.Unmarshal([]byte(got.Stdout), &parsed); err != nil {
		t.Fatalf("%v: %q", err, got.Stdout)
	}
	app := parsed["application-status"]
	units := app["units"].(map[string]any)
	if app["status"] != "waiting" || app["message"] != "for peers" || units["app/0"].(map[string]any)["status"] != "active" {
		t.Fatalf("%v", parsed)
	}
	// Without data and in a machine format: status and units only.
	got = rcall(r, "", "status-get", "--application", "--format=json")
	if !strings.Contains(got.Stdout, `"application-status":{"status":"waiting","units":{"app/0":{"status":"active"}}}`) {
		t.Fatalf("%q", got.Stdout)
	}
}

func TestApplicationVersionSet(t *testing.T) {
	r, f := newReal(t)
	if got := rcall(r, "", "application-version-set", "14.9"); got.Code != 0 || f.version != "14.9" {
		t.Fatalf("%+v %q", got, f.version)
	}
	if rcall(r, "", "application-version-set").Code == 0 || rcall(r, "", "application-version-set", "a", "b").Code == 0 {
		t.Fatal("bad arity accepted")
	}
	f.err = errors.New("nope")
	if rcall(r, "", "application-version-set", "1").Code == 0 {
		t.Fatal("backend error hidden")
	}
}

func TestJujuLog(t *testing.T) {
	r, f := newReal(t)
	tests := []struct {
		args []string
		want string
	}{
		{[]string{"hello", "world"}, "INFO: hello world"},
		{[]string{"-l", "warning", "careful"}, "WARNING: careful"},
		{[]string{"--log-level=ERROR", "bad"}, "ERROR: bad"},
		{[]string{"--debug", "dbg"}, "DEBUG: dbg"},
		{[]string{"msg", "--debug"}, "DEBUG: msg"},
		{[]string{"-l", "shouting", "m"}, "INFO: m"},
		{[]string{"--", "-not-a-flag"}, "INFO: -not-a-flag"},
	}
	for _, tt := range tests {
		f.logs = nil
		if got := rcall(r, "", "juju-log", tt.args...); got.Code != 0 {
			t.Fatalf("%v: %+v", tt.args, got)
		}
		if got := f.logs[len(f.logs)-1]; got != tt.want {
			t.Errorf("juju-log %v logged %q, want %q", tt.args, got, tt.want)
		}
	}
	if len(f.logs) != 2 || !strings.Contains(f.logs[0], "not valid") {
		// the invalid level case warns first
		t.Logf("logs: %v", f.logs)
	}
	if rcall(r, "", "juju-log").Code == 0 {
		t.Fatal("empty message accepted")
	}
	if got := rcall(r, "", "juju-log", "--format=json", "x"); got.Code != 0 || !strings.Contains(got.Stderr, "deprecated") {
		t.Fatalf("%+v", got)
	}
}

func TestStateTools(t *testing.T) {
	r, f := newReal(t)
	// Missing key: nothing, exit 0; strict fails; json prints "".
	if got := rcall(r, "", "state-get", "missing"); got.Code != 0 || got.Stdout != "" {
		t.Fatalf("%+v", got)
	}
	if got := rcall(r, "", "state-get", "missing", "--strict"); got.Code == 0 || !strings.Contains(got.Stderr, "not found") {
		t.Fatalf("%+v", got)
	}
	if got := rcall(r, "", "state-get", "missing", "--format=json"); got.Stdout != "\"\"\n" {
		t.Fatalf("%+v", got)
	}
	// Empty state: null in json, nothing otherwise (juju 4).
	if got := rcall(r, "", "state-get", "--format=json"); got.Stdout != "null\n" {
		t.Fatalf("%+v", got)
	}
	if got := rcall(r, "", "state-get", "-", "--format=json"); got.Stdout != "null\n" {
		t.Fatalf("%+v", got)
	}
	if got := rcall(r, "", "state-get"); got.Code != 0 || got.Stdout != "" {
		t.Fatalf("%+v", got)
	}
	// state-set: key=value pairs, empty values, and --file with stdin or a path; positionals win over the file.
	if got := rcall(r, "", "state-set", "a=1", "b=", "c=x=y"); got.Code != 0 {
		t.Fatalf("%+v", got)
	}
	if !reflect.DeepEqual(f.state, map[string]string{"a": "1", "b": "", "c": "x=y"}) {
		t.Fatalf("%v", f.state)
	}
	r.ReadFile = func(p string) ([]byte, error) {
		if p == "/some/file" {
			return []byte("d: 4\ne: true\nf: 1.5\ng: ~\nh: text\n"), nil
		}
		return nil, errors.New("no such file")
	}
	if got := rcall(r, "a: from-stdin\nz: 26\n", "state-set", "--file", "-", "a=positional"); got.Code != 0 {
		t.Fatalf("%+v", got)
	}
	if f.state["a"] != "positional" || f.state["z"] != "26" {
		t.Fatalf("%v", f.state)
	}
	if got := rcall(r, "", "state-set", "--file=/some/file"); got.Code != 0 {
		t.Fatalf("%+v", got)
	}
	if f.state["d"] != "4" || f.state["e"] != "true" || f.state["f"] != "1.5" || f.state["g"] != "" || f.state["h"] != "text" {
		t.Fatalf("%v", f.state)
	}
	for _, bad := range [][]string{{"novalue"}, {"=x"}, {"--file", "/missing"}} {
		if got := rcall(r, "", "state-set", bad...); got.Code == 0 {
			t.Errorf("state-set %v accepted", bad)
		}
	}
	if got := rcall(r, "- not\n- a map\n", "state-set", "--file", "-"); got.Code == 0 {
		t.Error("non-map file accepted")
	}
	// Reads.
	if got := rcall(r, "", "state-get", "a"); got.Stdout != "positional\n" {
		t.Fatalf("%+v", got)
	}
	if got := rcall(r, "", "state-get", "a", "--format=json"); got.Stdout != "\"positional\"\n" {
		t.Fatalf("%+v", got)
	}
	got := rcall(r, "", "state-get", "--format=json")
	var all map[string]string
	if err := json.Unmarshal([]byte(got.Stdout), &all); err != nil || all["c"] != "x=y" || len(all) != len(f.state) {
		t.Fatalf("%v %q", err, got.Stdout)
	}
	if got := rcall(r, "", "state-get", "a", "b"); got.Code == 0 {
		t.Fatal("extra args accepted")
	}
	if got := rcall(r, "", "state-get"); !strings.Contains(got.Stdout, "a: positional\n") {
		t.Fatalf("yaml state: %q", got.Stdout)
	}
	// Delete.
	if got := rcall(r, "", "state-delete", "a"); got.Code != 0 {
		t.Fatalf("%+v", got)
	}
	if _, ok := f.state["a"]; ok {
		t.Fatal("not deleted")
	}
	if rcall(r, "", "state-delete").Code == 0 || rcall(r, "", "state-delete", "a", "b").Code == 0 {
		t.Fatal("bad arity accepted")
	}
	f.err = errors.New("nope")
	if rcall(r, "", "state-set", "k=v").Code == 0 || rcall(r, "", "state-delete", "k").Code == 0 {
		t.Fatal("backend errors hidden")
	}
}

func TestParsePort(t *testing.T) {
	tests := []struct {
		in   string
		want Port
		err  bool
	}{
		{"80", Port{"tcp", 80, 80}, false},
		{"80/TCP", Port{"tcp", 80, 80}, false},
		{"53/udp", Port{"udp", 53, 53}, false},
		{"8000-8010/udp", Port{"udp", 8000, 8010}, false},
		{"1000-2000", Port{"tcp", 1000, 2000}, false},
		{"icmp", Port{Protocol: "icmp"}, false},
		{"ICMP", Port{Protocol: "icmp"}, false},
		{"0", Port{}, true},
		{"70000", Port{}, true},
		{"10-5", Port{}, true},
		{"abc", Port{}, true},
		{"5-x", Port{}, true},
		{"80/sctp", Port{}, true},
		{"", Port{}, true},
	}
	for _, tt := range tests {
		got, err := ParsePort(tt.in)
		if (err != nil) != tt.err || (err == nil && got != tt.want) {
			t.Errorf("ParsePort(%q) = %+v, %v", tt.in, got, err)
		}
	}
	for p, want := range map[Port]string{{"tcp", 80, 80}: "80/tcp", {"udp", 1, 9}: "1-9/udp", {Protocol: "icmp"}: "icmp"} {
		if p.String() != want {
			t.Errorf("%+v = %s", p, p.String())
		}
	}
}

func TestPortTools(t *testing.T) {
	r, f := newReal(t)
	if got := rcall(r, "", "open-port", "80/tcp"); got.Code != 0 || got.Stdout != "" {
		t.Fatalf("%+v", got)
	}
	rcall(r, "", "close-port", "9000-9010/udp")
	rcall(r, "", "open-port", "icmp", "--endpoints", "a,b")
	rcall(r, "", "open-port", "--endpoints=a", "81")
	if !reflect.DeepEqual(f.pending, []string{"open 80/tcp", "close 9000-9010/udp", "open icmp", "open 81/tcp"}) {
		t.Fatalf("%v", f.pending)
	}
	for _, bad := range [][]string{{}, {"nope"}, {"80", "81"}, {"--bogus", "80"}} {
		if rcall(r, "", "open-port", bad...).Code == 0 {
			t.Errorf("open-port %v accepted", bad)
		}
	}
	if got := rcall(r, "", "open-port", "80", "--format=json"); got.Code != 0 || !strings.Contains(got.Stderr, "deprecated") {
		t.Fatalf("%+v", got)
	}
	f.err = errors.New("x")
	if rcall(r, "", "open-port", "80").Code == 0 || rcall(r, "", "close-port", "80").Code == 0 {
		t.Fatal("backend errors hidden")
	}

	f.ports = []Port{{"udp", 53, 53}, {"tcp", 8080, 8088}, {"tcp", 80, 80}, {Protocol: "icmp"}}
	tests := []struct {
		args []string
		want string
	}{
		{nil, "icmp\n80/tcp\n8080-8088/tcp\n53/udp\n"},
		{[]string{"--format=json"}, `["icmp","80/tcp","8080-8088/tcp","53/udp"]` + "\n"},
		{[]string{"--endpoints"}, "icmp (*)\n80/tcp (*)\n8080-8088/tcp (*)\n53/udp (*)\n"},
	}
	for _, tt := range tests {
		if got := rcall(r, "", "opened-ports", tt.args...); got.Code != 0 || got.Stdout != tt.want {
			t.Errorf("opened-ports %v = %q, want %q", tt.args, got.Stdout, tt.want)
		}
	}
	f.ports = nil
	if got := rcall(r, "", "opened-ports"); got.Stdout != "" || got.Code != 0 {
		t.Errorf("%+v", got)
	}
	if got := rcall(r, "", "opened-ports", "--format=json"); got.Stdout != "[]\n" {
		t.Errorf("%+v", got)
	}
	if rcall(r, "", "opened-ports", "x").Code == 0 {
		t.Error("args accepted")
	}
}

func TestNetworkAndUnitGet(t *testing.T) {
	r, f := newReal(t)
	f.addRel(3, "db", "other")
	got := rcall(r, "", "network-get", "db", "--format=json")
	var parsed struct {
		Bind []struct {
			Name  string `json:"interface-name"`
			Addrs []struct {
				Value string `json:"value"`
				CIDR  string `json:"cidr"`
			} `json:"addresses"`
		} `json:"bind-addresses"`
		Egress  []string `json:"egress-subnets"`
		Ingress []string `json:"ingress-addresses"`
	}
	if err := json.Unmarshal([]byte(got.Stdout), &parsed); err != nil {
		t.Fatalf("%v: %q", err, got.Stdout)
	}
	if len(parsed.Bind) != 1 || parsed.Bind[0].Addrs[0].Value != "10.1.2.3" || parsed.Bind[0].Addrs[0].CIDR != "10.1.2.3/32" ||
		parsed.Egress[0] != "10.1.2.3/32" || parsed.Ingress[0] != "10.1.2.3" {
		t.Fatalf("%+v", parsed)
	}
	tests := []struct {
		args []string
		want string
	}{
		{[]string{"db", "--bind-address"}, "10.1.2.3\n"},
		{[]string{"--ingress-address", "db"}, "10.1.2.3\n"},
		{[]string{"db", "--egress-subnets", "--format=json"}, `["10.1.2.3/32"]` + "\n"},
		{[]string{"db", "--primary-address"}, "10.1.2.3\n"},
		{[]string{"db", "--bind-address", "--ingress-address", "--format=json"}, `{"bind-address":"10.1.2.3","ingress-address":"10.1.2.3"}` + "\n"},
		{[]string{"db", "-r", "3", "--ingress-address"}, "10.1.2.3\n"},
	}
	for _, tt := range tests {
		if got := rcall(r, "", "network-get", tt.args...); got.Code != 0 || got.Stdout != tt.want {
			t.Errorf("network-get %v = %+v, want %q", tt.args, got, tt.want)
		}
	}
	for _, bad := range [][]string{{}, {""}, {"db", "extra"}, {"db", "--primary-address", "--bind-address"}} {
		if rcall(r, "", "network-get", bad...).Code == 0 {
			t.Errorf("network-get %v accepted", bad)
		}
	}
	for _, name := range []string{"private-address", "public-address"} {
		if got := rcall(r, "", "unit-get", name); got.Stdout != "10.1.2.3\n" {
			t.Errorf("unit-get %s: %+v", name, got)
		}
	}
	for _, bad := range [][]string{{}, {"nope"}, {"private-address", "x"}} {
		if rcall(r, "", "unit-get", bad...).Code == 0 {
			t.Errorf("unit-get %v accepted", bad)
		}
	}
	f.addr = ""
	if rcall(r, "", "network-get", "db").Code == 0 {
		t.Error("network-get without an address succeeded")
	}
}

func TestUnsupportedTools(t *testing.T) {
	r, _ := newReal(t)
	for _, name := range []string{"action-get", "action-set", "action-fail", "action-log"} {
		if got := rcall(r, "", name, "x=y"); got.Code != 1 || !strings.Contains(got.Stderr, "not running an action") {
			t.Errorf("%s: %+v", name, got)
		}
	}
	if got := rcall(r, "", "action-fail"); got.Code != 1 || !strings.Contains(got.Stderr, "not running an action") {
		t.Errorf("action-fail: %+v", got)
	}
	for name, want := range map[string]string{"juju-reboot": "not supported", "credential-get": "credential-get is not supported",
		"resource-get": "resource-get is not supported", "storage-add": "not supported on k8s"} {
		if got := rcall(r, "", name); got.Code != 1 || !strings.Contains(got.Stderr, want) {
			t.Errorf("%s: %+v", name, got)
		}
	}
	if got := rcall(r, "", "no-such-tool"); got.Code != 1 || !strings.Contains(got.Stderr, "unknown hook tool") {
		t.Errorf("%+v", got)
	}
	// Every tool in Names is handled.
	for _, n := range Names {
		if got := rcall(r, "", n); strings.Contains(got.Stderr, "unknown hook tool") {
			t.Errorf("%s unhandled", n)
		}
	}
}

func TestParseCLI(t *testing.T) {
	def := flagDef{flags: map[string]bool{"format": true, "all": false, "n": true}, aliases: map[string]string{"a": "all"}}
	c, err := parseCLI(def, []string{"pos1", "-a", "--format", "json", "pos2", "-n=5", "-", "--", "--all", "x"})
	if err != nil {
		t.Fatal(err)
	}
	if !c.boolean("all") || c.str("format") != "json" || c.str("n") != "5" || !reflect.DeepEqual(c.pos, []string{"pos1", "pos2", "-", "--all", "x"}) {
		t.Fatalf("%+v", c)
	}
	c, _ = parseCLI(def, []string{"--all=false"})
	if c.boolean("all") || !c.has("all") {
		t.Fatalf("%+v", c)
	}
	c, _ = parseCLI(def, []string{"--all=False"})
	if c.boolean("all") {
		t.Fatal("no")
	}
	if _, err := parseCLI(def, []string{"--zzz"}); err == nil {
		t.Fatal("unknown flag")
	}
	if _, err := parseCLI(def, []string{"--zzz=1"}); err == nil || !strings.Contains(err.Error(), "--zzz") || strings.Contains(err.Error(), "=1") {
		t.Fatal(err)
	}
	if _, err := parseCLI(def, []string{"-n"}); err == nil {
		t.Fatal("missing value")
	}
	if formatYAML(nil) != "" || formatSmart(nil) != "" || formatSmart("") != "" || formatSmart([]string{}) != "" {
		t.Fatal("empty output")
	}
	if formatSmart([]string{"a", "b"}) != "a\nb\n" || formatSmart(7) != "7\n" {
		t.Fatal("smart")
	}
	if got := write(cli{vals: map[string]string{}}, "json", make(chan int)); got.Code == 0 {
		t.Fatal("unmarshalable value accepted")
	}
	if formatYAML(make(chan int)) != "" {
		t.Fatal("unmarshalable yaml")
	}
}
