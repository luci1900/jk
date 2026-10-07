// SPDX-License-Identifier: AGPL-3.0-only

package hooktools

import (
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

const xid = "cnj3q47mp25c7a0e7klg"
const uri = "secret:" + xid

func secretReal(t *testing.T) (*Real, *fakeBackend) {
	r, f := newReal(t)
	f.leader = true
	f.secrets[xid] = SecretMetadata{Label: "db-password", Owner: OwnerApplication, Description: "the password", LatestRevision: 3,
		Access: []SecretAccess{{Target: "application-app", Scope: "relation-1", Role: "view"}}}
	f.content[xid] = map[string][]byte{"password": []byte("s3cret"), "user": []byte("admin")}
	f.addRel(7, "database-peers", "app", "app/1")
	return r, f
}

func TestParseSecretURI(t *testing.T) {
	good := map[string]string{
		xid:             xid,
		"secret:" + xid: xid,
		"secret://11111111-2222-3333-4444-555555555555/" + xid: xid,
		"secret:9m4e2mr0ui3e8a215n4g":                          "9m4e2mr0ui3e8a215n4g",
	}
	for in, want := range good {
		if got, err := ParseSecretURI(in); err != nil || got != want {
			t.Errorf("ParseSecretURI(%q) = %q, %v", in, got, err)
		}
	}
	for _, bad := range []string{"", "secret:", "secret:short", "secret:" + xid + "x", "secret:CNJ3Q47MP25C7A0E7KLG", "http://x/" + xid,
		"secret://nouuid/" + xid, "secret:cnj3q47mp25c7a0e7klz", "secret:cnj3q47mp25c7a0e7kl1"} {
		if _, err := ParseSecretURI(bad); err == nil {
			t.Errorf("ParseSecretURI(%q) accepted", bad)
		}
	}
	if SecretURI(xid) != uri {
		t.Fatal("SecretURI")
	}
}

func TestSecretAdd(t *testing.T) {
	r, f := secretReal(t)
	dir := t.TempDir()
	file := filepath.Join(dir, "cert")
	if err := os.WriteFile(file, []byte("line1\nline2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	yamlFile := filepath.Join(dir, "content.yaml")
	if err := os.WriteFile(yamlFile, []byte("string42: 42\nnull-value: null\nkey#base64: AA==\nplain: text\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	got := rcall(r, "", "secret-add", "--label", "db-password", "--description=the password", "--owner", "unit", "--rotate", "monthly",
		"--expire", "24h", "token=34ae35facd4", "key#base64=AA==", "cert#file="+file, "padded=ab==", "equ=a=b")
	if got.Code != 0 || got.Stdout != "secret:cnj3q47mp25c7a0e7kl0\n" {
		t.Fatalf("%+v", got)
	}
	c := f.created[0]
	want := map[string][]byte{"token": []byte("34ae35facd4"), "key": {0}, "cert": []byte("line1\nline2\n"), "padded": []byte("ab=="), "equ": []byte("a=b")}
	if !reflect.DeepEqual(c.Content, want) {
		t.Fatalf("content %q, want %q", c.Content, want)
	}
	if c.Owner != OwnerUnit || c.Label != "db-password" || c.Description != "the password" || c.RotatePolicy != "monthly" ||
		c.Expire == nil || !c.Expire.Equal(time.Date(2026, 1, 3, 3, 4, 5, 0, time.UTC)) {
		t.Fatalf("%+v", c)
	}

	// Defaults: application owner.
	rcall(r, "", "secret-add", "a-key=v")
	if f.created[1].Owner != OwnerApplication {
		t.Fatalf("%+v", f.created[1])
	}

	// --file: YAML or JSON map; values are coerced to strings; #base64 keys are decoded; arguments add to it.
	if got := rcall(r, "", "secret-add", "--file="+yamlFile, "extra=1"); got.Code != 0 {
		t.Fatalf("%+v", got)
	}
	if !reflect.DeepEqual(f.created[2].Content, map[string][]byte{"string42": []byte("42"), "null-value": {}, "key": {0}, "plain": []byte("text"), "extra": []byte("1")}) {
		t.Fatalf("%q", f.created[2].Content)
	}
	// --file - reads stdin.
	if got := rcall(r, `{"from-stdin": "yes"}`, "secret-add", "--file", "-"); got.Code != 0 || string(f.created[3].Content["from-stdin"]) != "yes" {
		t.Fatalf("%+v %q", got, f.created[3].Content)
	}

	// Expiry formats.
	for spec, wantT := range map[string]time.Time{
		"2030-01-02T06:06:06Z":      time.Date(2030, 1, 2, 6, 6, 6, 0, time.UTC),
		"2030-01-02T06:06:06":       time.Date(2030, 1, 2, 6, 6, 6, 0, time.UTC),
		"2030-01-02T06:06:06+02:00": time.Date(2030, 1, 2, 4, 6, 6, 0, time.UTC),
		"90m":                       time.Date(2026, 1, 2, 4, 34, 5, 0, time.UTC),
	} {
		rcall(r, "", "secret-add", "--expire", spec, "a-key=v")
		if e := f.created[len(f.created)-1].Expire; e == nil || !e.Equal(wantT) {
			t.Errorf("--expire %s = %v, want %v", spec, e, wantT)
		}
	}

	// Errors.
	for name, args := range map[string][]string{
		"no value":          {},
		"empty":             {"--label", "x"},
		"bad key":           {"Bad=1"},
		"short key":         {"ab=1"},
		"no equals":         {"novalue"},
		"empty key":         {"=v"},
		"bad base64":        {"key#base64=!!!"},
		"bad expire":        {"--expire", "tomorrow", "a-key=v"},
		"negative expire":   {"--expire", "-1h", "a-key=v"},
		"bad rotate":        {"--rotate", "sometimes", "a-key=v"},
		"bad owner":         {"--owner", "model", "a-key=v"},
		"missing file":      {"x-key#file=/no/such/file"},
		"missing --file":    {"--file", "/no/such/file"},
		"unknown flag":      {"--nope", "a-key=v"},
		"too large":         {"big-key=" + strings.Repeat("x", 1000*1000+1)},
		"too large overall": {"aaa=" + strings.Repeat("x", 600000), "bbb=" + strings.Repeat("y", 600000)},
	} {
		if got := rcall(r, "", "secret-add", args...); got.Code == 0 {
			t.Errorf("%s: accepted: %+v", name, got)
		}
	}
	// Errors from the backend are reported.
	f.secretErr = ErrNotLeader
	if got := rcall(r, "", "secret-add", "a-key=v"); got.Code == 0 || !strings.Contains(got.Stderr, "not the leader") {
		t.Errorf("%+v", got)
	}
}

func TestSecretSet(t *testing.T) {
	r, f := secretReal(t)
	if got := rcall(r, "", "secret-set", uri, "token=new", "--label=l2", "--description", "d2", "--rotate", "daily", "--expire", "2030-01-02T06:06:06Z", "--owner", "application"); got.Code != 0 {
		t.Fatalf("%+v", got)
	}
	u := f.updates[uri]
	if string(u.Content["token"]) != "new" || *u.Label != "l2" || *u.Description != "d2" || *u.RotatePolicy != "daily" || u.Expire == nil {
		t.Fatalf("%+v", u)
	}
	// Metadata only.
	delete(f.updates, uri)
	rcall(r, "", "secret-set", "--label", "only", uri)
	if u := f.updates[uri]; len(u.Content) != 0 || *u.Label != "only" || u.Description != nil || u.RotatePolicy != nil || u.Expire != nil {
		t.Fatalf("%+v label=%q", u, *u.Label)
	}
	// Content from a file.
	rcall(r, "abc: b\n", "secret-set", uri, "--file=-")
	if string(f.updates[uri].Content["abc"]) != "b" {
		t.Fatalf("%+v", f.updates[uri])
	}
	// Nothing to change is a no-op.
	delete(f.updates, uri)
	if got := rcall(r, "", "secret-set", uri); got.Code != 0 || len(f.updates) != 0 {
		t.Fatalf("%+v %v", got, f.updates)
	}
	for name, args := range map[string][]string{
		"no id":     {},
		"bad id":    {"secret:nope", "a-key=v"},
		"bad value": {uri, "Bad=1"},
		"bad owner": {uri, "--owner=x", "a-key=v"},
	} {
		if got := rcall(r, "", "secret-set", args...); got.Code == 0 {
			t.Errorf("%s: accepted", name)
		}
	}
	f.secretErr = ErrNotLeader
	if got := rcall(r, "", "secret-set", uri, "a-key=v"); got.Code == 0 {
		t.Errorf("backend error swallowed")
	}
}

func TestSecretGet(t *testing.T) {
	r, f := secretReal(t)
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"yaml is the default", []string{uri}, "password: s3cret\nuser: admin\n"},
		{"json", []string{uri, "--format=json"}, `{"password":"s3cret","user":"admin"}` + "\n"},
		{"flags first, as ops sends them", []string{"--format=json", uri}, `{"password":"s3cret","user":"admin"}` + "\n"},
		{"bare id", []string{xid, "--format", "json"}, `{"password":"s3cret","user":"admin"}` + "\n"},
		{"by label", []string{"--label", "db-password", "--format=json"}, `{"password":"s3cret","user":"admin"}` + "\n"},
		{"one key", []string{uri, "password"}, "s3cret\n"},
		{"one key base64", []string{uri, "password#base64"}, base64.StdEncoding.EncodeToString([]byte("s3cret")) + "\n"},
		{"one key json", []string{uri, "user", "--format=json"}, `"admin"` + "\n"},
		{"peek", []string{uri, "--peek", "--format=json"}, `{"password":"s3cret","user":"admin"}` + "\n"},
		{"refresh", []string{uri, "--refresh", "--format=json"}, `{"password":"s3cret","user":"admin"}` + "\n"},
		{"uri and label", []string{uri, "--label", "my-label", "--format=json"}, `{"password":"s3cret","user":"admin"}` + "\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := rcall(r, "", "secret-get", tt.args...)
			if got.Code != 0 || got.Stdout != tt.want {
				t.Fatalf("%+v, want %q", got, tt.want)
			}
		})
	}
	wantCalls := []getCall{{uri, "", false, false}, {uri, "", false, false}, {uri, "", false, false}, {uri, "", false, false}, {"", "db-password", false, false},
		{uri, "", false, false}, {uri, "", false, false}, {uri, "", false, false}, {uri, "", false, true}, {uri, "", true, false}, {uri, "my-label", false, false}}
	if !reflect.DeepEqual(f.getCalls, wantCalls) {
		t.Fatalf("calls %+v\nwant %+v", f.getCalls, wantCalls)
	}

	// Errors: a missing secret says "not found" (ops maps it to SecretNotFoundError).
	f.getCalls = nil
	for name, tc := range map[string]struct {
		args []string
		msg  string
	}{
		"nothing given":    {[]string{}, "require either a secret URI or label"},
		"peek and refresh": {[]string{uri, "--peek", "--refresh"}, "specify one of --peek or --refresh but not both"},
		"bad uri":          {[]string{"secret:x"}, "not valid"},
		"unknown key":      {[]string{uri, "nokey"}, "not found"},
		"unknown key b64":  {[]string{uri, "nokey#base64"}, "not found"},
		"extra":            {[]string{uri, "a", "b"}, "unrecognized args"},
		"bad format":       {[]string{uri, "--format=smart"}, "unknown format"},
		"missing":          {[]string{"secret:aaaaaaaaaaaaaaaaaaa0", "--format=json"}, "not found"},
		"missing label":    {[]string{"--label", "nope", "--format=json"}, "not found"},
	} {
		got := rcall(r, "", "secret-get", tc.args...)
		if got.Code == 0 || !strings.Contains(got.Stderr, tc.msg) {
			t.Errorf("%s: %+v", name, got)
		}
	}
	f.secretErr = ErrPermission
	if got := rcall(r, "", "secret-get", uri); got.Code == 0 || !strings.Contains(got.Stderr, "permission denied") {
		t.Errorf("%+v", got)
	}
}

func TestSecretInfoGet(t *testing.T) {
	r, f := secretReal(t)
	exp := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	md := f.secrets[xid]
	md.Expire, md.RotatePolicy = &exp, "monthly"
	f.secrets[xid] = md
	f.secrets["aaaaaaaaaaaaaaaaaaa0"] = SecretMetadata{Label: "unit-secret", Owner: OwnerUnit, LatestRevision: 1}

	got := rcall(r, "", "secret-info-get", uri, "--format=json")
	want := `{"` + xid + `":{"revision":3,"label":"db-password","owner":"application","description":"the password","rotation":"monthly",` +
		`"expiry":"2030-01-02T03:04:05Z","access":[{"target":"application-app","scope":"relation-1","role":"view"}]}}` + "\n"
	if got.Code != 0 || got.Stdout != want {
		t.Fatalf("%+v\nwant %s", got, want)
	}
	// yaml by label (the default format).
	got = rcall(r, "", "secret-info-get", "--label", "unit-secret")
	if got.Code != 0 || got.Stdout != "aaaaaaaaaaaaaaaaaaa0:\n  label: unit-secret\n  owner: unit\n  revision: 1\n" {
		t.Fatalf("%+v", got)
	}
	// Empty optional fields are omitted; the label is not.
	got = rcall(r, "", "secret-info-get", "aaaaaaaaaaaaaaaaaaa0", "--format=json")
	if got.Stdout != `{"aaaaaaaaaaaaaaaaaaa0":{"revision":1,"label":"unit-secret","owner":"unit"}}`+"\n" {
		t.Fatalf("%+v", got)
	}
	for name, args := range map[string][]string{
		"nothing":     {},
		"both":        {uri, "--label", "x"},
		"unknown id":  {"secret:bbbbbbbbbbbbbbbbbbb0"},
		"unknown lbl": {"--label", "nope"},
		"bad uri":     {"secret:x"},
		"extra":       {uri, "x"},
		"bad format":  {uri, "--format=smart"},
	} {
		if got := rcall(r, "", "secret-info-get", args...); got.Code == 0 {
			t.Errorf("%s: accepted: %+v", name, got)
		}
	}
	f.secretErr = errors.New("boom")
	if got := rcall(r, "", "secret-info-get", uri); got.Code == 0 {
		t.Error("backend error swallowed")
	}
}

func TestSecretIDs(t *testing.T) {
	r, f := secretReal(t)
	f.secrets["aaaaaaaaaaaaaaaaaaa0"] = SecretMetadata{Label: "other"}
	if got := rcall(r, "", "secret-ids"); got.Stdout != "aaaaaaaaaaaaaaaaaaa0\n"+xid+"\n" {
		t.Errorf("%+v", got)
	}
	if got := rcall(r, "", "secret-ids", "--format=json"); got.Stdout != `["aaaaaaaaaaaaaaaaaaa0","`+xid+`"]`+"\n" {
		t.Errorf("%+v", got)
	}
	f.secrets = map[string]SecretMetadata{}
	if got := rcall(r, "", "secret-ids", "--format=json"); got.Stdout != "[]\n" {
		t.Errorf("%+v", got)
	}
	if rcall(r, "", "secret-ids", "x").Code == 0 {
		t.Error("args accepted")
	}
	f.secretErr = errors.New("boom")
	if rcall(r, "", "secret-ids").Code == 0 {
		t.Error("backend error swallowed")
	}
}

func TestSecretRemove(t *testing.T) {
	r, f := secretReal(t)
	if got := rcall(r, "", "secret-remove", uri); got.Code != 0 {
		t.Fatalf("%+v", got)
	}
	if revs, ok := f.removed[uri]; !ok || revs != nil {
		t.Fatalf("%v", f.removed)
	}
	delete(f.removed, uri)
	rcall(r, "", "secret-remove", uri, "--revision", "2")
	rcall(r, "", "secret-remove", "--revision=3", uri)
	if !reflect.DeepEqual(f.removed[uri], []int{2, 3}) {
		t.Fatalf("%v", f.removed)
	}
	// --revision 0 removes everything, as in juju.
	delete(f.removed, uri)
	rcall(r, "", "secret-remove", uri, "--revision=0")
	if revs, ok := f.removed[uri]; !ok || revs != nil {
		t.Fatalf("%v", f.removed)
	}
	for _, args := range [][]string{{}, {"secret:x"}, {uri, "x"}, {uri, "--revision", "abc"}} {
		if got := rcall(r, "", "secret-remove", args...); got.Code == 0 {
			t.Errorf("%v accepted", args)
		}
	}
	f.secretErr = ErrNotLeader
	if rcall(r, "", "secret-remove", uri).Code == 0 {
		t.Error("backend error swallowed")
	}
}

func TestSecretGrantAndRevoke(t *testing.T) {
	r, f := secretReal(t)
	if got := rcall(r, "", "secret-grant", uri, "-r", "7"); got.Code != 0 {
		t.Fatalf("%+v", got)
	}
	if got := rcall(r, "", "secret-grant", "--relation", "database-peers:7", "--unit", "app/1", uri); got.Code != 0 {
		t.Fatalf("%+v", got)
	}
	want := []SecretGrant{{Application: "app", Relation: "7"}, {Application: "app", Unit: "app/1", Relation: "7"}}
	if !reflect.DeepEqual(f.grants[uri], want) {
		t.Fatalf("%+v", f.grants[uri])
	}
	// The relation defaults to the hook's.
	f.hookRel = &HookRelation{ID: 7, Endpoint: "database-peers", RemoteApp: "app"}
	rcall(r, "", "secret-grant", uri)
	if len(f.grants[uri]) != 3 {
		t.Fatalf("%+v", f.grants[uri])
	}
	f.hookRel = nil
	for name, args := range map[string][]string{
		"no id":         {},
		"no relation":   {uri},
		"unknown rel":   {uri, "-r", "70"},
		"unit of other": {uri, "-r", "7", "--unit", "other/1"},
		"bad unit":      {uri, "-r", "7", "--unit", "nonsense"},
		"bad uri":       {"secret:x", "-r", "7"},
		"extra":         {uri, "-r", "7", "x"},
	} {
		if got := rcall(r, "", "secret-grant", args...); got.Code == 0 {
			t.Errorf("%s: accepted", name)
		}
	}
	f.secretErr = ErrNotLeader
	if rcall(r, "", "secret-grant", uri, "-r", "7").Code == 0 {
		t.Error("backend error swallowed")
	}
	f.secretErr = nil

	// Revoke.
	for _, args := range [][]string{{uri, "--app", "app"}, {uri, "--application=app"}, {uri, "--unit", "app/1"}, {uri, "-r", "7"}} {
		if got := rcall(r, "", "secret-revoke", args...); got.Code != 0 {
			t.Fatalf("%v: %+v", args, got)
		}
	}
	wantRev := []SecretGrant{{Application: "app"}, {Application: "app"}, {Unit: "app/1"}, {Application: "app", Relation: "7"}}
	if !reflect.DeepEqual(f.revoked[uri], wantRev) {
		t.Fatalf("%+v", f.revoked[uri])
	}
	for name, args := range map[string][]string{
		"no id":        {},
		"nothing":      {uri},
		"app and unit": {uri, "--app", "a", "--unit", "a/1"},
		"rel and app":  {uri, "-r", "7", "--app", "app"},
		"unknown rel":  {uri, "-r", "70"},
		"bad app":      {uri, "--app", "a/1"},
		"bad unit":     {uri, "--unit", "x"},
		"extra":        {uri, "--app", "a", "x"},
		"bad uri":      {"nope", "--app", "a"},
	} {
		if got := rcall(r, "", "secret-revoke", args...); got.Code == 0 {
			t.Errorf("%s: accepted", name)
		}
	}
	f.secretErr = ErrNotLeader
	if rcall(r, "", "secret-revoke", uri, "--app", "app").Code == 0 {
		t.Error("backend error swallowed")
	}
}
