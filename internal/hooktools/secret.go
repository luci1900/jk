// SPDX-License-Identifier: AGPL-3.0-only
//
// Derived from juju (https://github.com/juju/juju), AGPL-3.0: the secret-* tools of
// internal/worker/uniter/runner/jujuc and core/secrets (CreateSecretData, ParseURI, SecretValue).

package hooktools

import (
	"encoding/base64"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	maxSecretValueBytes   = 1000 * 1000
	maxSecretContentBytes = 1000 * 1000
)

var (
	secretKeyRE = regexp.MustCompile(`^([a-z](?:-?[a-z0-9]){2,})$`)
	// A secret id is an xid: 20 base32hex characters, the last carrying one bit.
	secretIDRE = regexp.MustCompile(`^[0-9a-v]{19}[0g]$`)
	uuidRE     = regexp.MustCompile(`^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$`)
)

// SecretURI formats a secret id as the URI charms see.
func SecretURI(xid string) string { return "secret:" + xid }

// ParseSecretURI accepts secret:<xid>, secret://<model uuid>/<xid> and a bare <xid>, and returns the xid.
func ParseSecretURI(s string) (string, error) {
	rest := s
	if after, ok := strings.CutPrefix(rest, "secret:"); ok {
		rest = after
	}
	if after, ok := strings.CutPrefix(rest, "//"); ok {
		host, id, found := strings.Cut(after, "/")
		if !found || !uuidRE.MatchString(host) {
			return "", fmt.Errorf("secret URI %q not valid", s)
		}
		rest = id
	}
	if !secretIDRE.MatchString(rest) {
		return "", fmt.Errorf("secret URI %q not valid", s)
	}
	return rest, nil
}

// parseSecretContent is juju's CreateSecretData plus ReadSecretData: key=value arguments (key#base64 for values
// already in base64, key#file for values read from a file) and an optional YAML or JSON file, decoded.
func (r *Real) parseSecretContent(args []string, file, stdin string) (map[string][]byte, error) {
	data := map[string][]byte{}
	var size int
	add := func(key string, value []byte) error {
		if !secretKeyRE.MatchString(key) {
			return fmt.Errorf("key %q not valid", key)
		}
		if len(value) > maxSecretValueBytes {
			return fmt.Errorf("secret content for key %q too large: %d bytes", key, len(value))
		}
		size += len(value)
		if size > maxSecretContentBytes {
			return fmt.Errorf("secret content too large: %d bytes", size)
		}
		data[key] = value
		return nil
	}
	setKV := func(key string, value string, fromFile bool) error {
		if before, ok := strings.CutSuffix(key, "#base64"); ok {
			b, err := base64.StdEncoding.DecodeString(value)
			if err != nil {
				return fmt.Errorf("key %q: %w", before, err)
			}
			return add(before, b)
		}
		return add(key, []byte(value))
	}
	for _, a := range args {
		// Remove any base64 padding ("=") before splitting the key=value.
		stripped := strings.TrimRight(a, "=")
		idx := strings.Index(stripped, "=")
		if idx < 1 {
			return nil, fmt.Errorf("key value %q not valid", a)
		}
		key, value := a[:idx], a[idx+1:]
		if before, ok := strings.CutSuffix(key, "#file"); ok {
			b, err := r.readFile(value, "")
			if err != nil {
				return nil, fmt.Errorf("reading content for secret key %q: %w", before, err)
			}
			if err := add(before, b); err != nil {
				return nil, err
			}
			continue
		}
		if err := setKV(key, value, false); err != nil {
			return nil, err
		}
	}
	if file != "" {
		b, err := r.readFile(file, stdin)
		if err != nil {
			return nil, err
		}
		kv, err := readSettings(b)
		if err != nil {
			return nil, err
		}
		keys := make([]string, 0, len(kv))
		for k := range kv {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if err := setKV(k, kv[k], true); err != nil {
				return nil, err
			}
		}
	}
	return data, nil
}

var rotatePolicies = map[string]bool{"never": true, "hourly": true, "daily": true, "weekly": true, "monthly": true, "quarterly": true, "yearly": true}

func parseExpire(spec string, now time.Time) (*time.Time, error) {
	if spec == "" {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339, spec)
	if err != nil {
		t, err = time.Parse("2006-01-02T15:04:05", spec)
	}
	if err != nil {
		d, derr := time.ParseDuration(spec)
		if derr != nil {
			return nil, fmt.Errorf("expire time or duration %q not valid", spec)
		}
		if d <= 0 {
			return nil, fmt.Errorf("negative expire duration %q not valid", spec)
		}
		t = now.Add(d)
	}
	t = t.UTC()
	return &t, nil
}

var upsertFlags = map[string]bool{"expire": true, "rotate": true, "description": true, "label": true, "file": true, "owner": true}

// upsert holds the parsed flags of secret-add and secret-set.
type upsert struct {
	owner, label, description, rotate string
	expire                            *time.Time
	content                           map[string][]byte
}

func (r *Real) parseUpsert(c cli, positional []string, stdin string) (upsert, error) {
	var u upsert
	var err error
	u.owner = c.str("owner")
	if u.owner == "" {
		u.owner = OwnerApplication
	}
	if u.owner != OwnerApplication && u.owner != OwnerUnit {
		return u, fmt.Errorf("secret owner %q not valid", u.owner)
	}
	u.label, u.description, u.rotate = c.str("label"), c.str("description"), c.str("rotate")
	if u.rotate != "" && !rotatePolicies[u.rotate] {
		return u, fmt.Errorf("rotate policy %q not valid", u.rotate)
	}
	if u.expire, err = parseExpire(c.str("expire"), r.now()); err != nil {
		return u, err
	}
	u.content, err = r.parseSecretContent(positional, c.str("file"), stdin)
	return u, err
}

func (r *Real) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *Real) secretAdd(req Request) Response {
	c, bad := parseReq(flagDef{flags: upsertFlags}, req)
	if bad != nil {
		return *bad
	}
	if len(c.pos) < 1 && c.str("file") == "" {
		return failf("missing secret value or filename")
	}
	u, err := r.parseUpsert(c, c.pos, req.Stdin)
	if err != nil {
		return failf("%v", err)
	}
	if len(u.content) == 0 {
		return failf("empty secret value not valid")
	}
	uri, err := r.B.CreateSecret(SecretCreate{Owner: u.owner, Label: u.label, Description: u.description,
		RotatePolicy: u.rotate, Expire: u.expire, Content: u.content})
	if err != nil {
		return failf("%v", err)
	}
	return Response{Stdout: uri + "\n"}
}

func (r *Real) secretSet(req Request) Response {
	c, bad := parseReq(flagDef{flags: upsertFlags}, req)
	if bad != nil {
		return *bad
	}
	if len(c.pos) < 1 {
		return failf("missing secret URI")
	}
	uri, err := ParseSecretURI(c.pos[0])
	if err != nil {
		return failf("%v", err)
	}
	u, err := r.parseUpsert(c, c.pos[1:], req.Stdin)
	if err != nil {
		return failf("%v", err)
	}
	upd := SecretUpdate{Expire: u.expire, Content: u.content}
	if u.label != "" {
		upd.Label = &u.label
	}
	if u.description != "" {
		upd.Description = &u.description
	}
	if u.rotate != "" {
		upd.RotatePolicy = &u.rotate
	}
	if upd.Label == nil && upd.Description == nil && upd.RotatePolicy == nil && upd.Expire == nil && len(upd.Content) == 0 {
		return Response{}
	}
	if err := r.B.UpdateSecret(SecretURI(uri), upd); err != nil {
		return failf("%v", err)
	}
	return Response{}
}

func (r *Real) secretGet(req Request) Response {
	c, bad := parseReq(flagDef{flags: map[string]bool{"format": true, "label": true, "peek": false, "refresh": false}}, req)
	if bad != nil {
		return *bad
	}
	if f := c.str("format"); f != "" && f != "yaml" && f != "json" {
		return failf("invalid value %q for flag --format: unknown format %q", f, f)
	}
	args := c.pos
	uri, label := "", c.str("label")
	if len(args) > 0 {
		id, err := ParseSecretURI(args[0])
		if err != nil {
			return failf("%v", err)
		}
		uri, args = SecretURI(id), args[1:]
	}
	if uri == "" && label == "" {
		return failf("require either a secret URI or label")
	}
	peek, refresh := c.boolean("peek"), c.boolean("refresh")
	if peek && refresh {
		return failf("specify one of --peek or --refresh but not both")
	}
	key := ""
	if len(args) > 0 {
		key = args[0]
		if len(args) > 1 {
			return failf("unrecognized args: %q", args[1:])
		}
	}
	content, err := r.B.GetSecret(uri, label, refresh, peek)
	if err != nil {
		return failf("%v", err)
	}
	if key == "" {
		out := make(map[string]string, len(content))
		for k, v := range content {
			out[k] = string(v)
		}
		return write(c, "yaml", out)
	}
	if k, ok := strings.CutSuffix(key, "#base64"); ok {
		v, found := content[k]
		if !found {
			return failf("secret key value %q not found", key)
		}
		return write(c, "yaml", base64.StdEncoding.EncodeToString(v))
	}
	v, ok := content[key]
	if !ok {
		return failf("secret key value %q not found", key)
	}
	return write(c, "yaml", string(v))
}

type secretInfoDisplay struct {
	LatestRevision   int            `json:"revision"`
	Label            string         `json:"label"`
	Owner            string         `json:"owner"`
	Description      string         `json:"description,omitempty"`
	RotatePolicy     string         `json:"rotation,omitempty"`
	LatestExpireTime *time.Time     `json:"expiry,omitempty"`
	Access           []SecretAccess `json:"access,omitempty"`
}

func (r *Real) secretInfoGet(req Request) Response {
	c, bad := parseReq(flagDef{flags: map[string]bool{"format": true, "label": true}}, req)
	if bad != nil {
		return *bad
	}
	if f := c.str("format"); f != "" && f != "yaml" && f != "json" {
		return failf("invalid value %q for flag --format: unknown format %q", f, f)
	}
	args := c.pos
	id, label := "", c.str("label")
	if len(args) > 0 {
		x, err := ParseSecretURI(args[0])
		if err != nil {
			return failf("%v", err)
		}
		id, args = x, args[1:]
	}
	if id == "" && label == "" {
		return failf("require either a secret URI or label")
	}
	if id != "" && label != "" {
		return failf("specify either a secret URI or label but not both")
	}
	if len(args) > 0 {
		return failf("unrecognized args: %q", args)
	}
	all, err := r.B.SecretMetadata()
	if err != nil {
		return failf("%v", err)
	}
	show := func(id string, md SecretMetadata) Response {
		return write(c, "yaml", map[string]secretInfoDisplay{id: {
			LatestRevision: md.LatestRevision, Label: md.Label, Owner: md.Owner, Description: md.Description,
			RotatePolicy: md.RotatePolicy, LatestExpireTime: md.Expire, Access: md.Access,
		}})
	}
	if id != "" {
		if md, ok := all[id]; ok {
			return show(id, md)
		}
		return failf("secret %q not found", id)
	}
	ids := make([]string, 0, len(all))
	for k := range all {
		ids = append(ids, k)
	}
	sort.Strings(ids)
	for _, k := range ids {
		if all[k].Label == label {
			return show(k, all[k])
		}
	}
	return failf("secret %q not found", label)
}

func (r *Real) secretIDs(req Request) Response {
	c, bad := parseReq(withFormat(nil, nil), req)
	if bad != nil {
		return *bad
	}
	if len(c.pos) > 0 {
		return failf("unrecognized args: %q", c.pos)
	}
	all, err := r.B.SecretMetadata()
	if err != nil {
		return failf("%v", err)
	}
	out := make([]string, 0, len(all))
	for id := range all {
		out = append(out, id)
	}
	sort.Strings(out)
	return write(c, "smart", out)
}

func (r *Real) secretRemove(req Request) Response {
	c, bad := parseReq(flagDef{flags: map[string]bool{"revision": true}}, req)
	if bad != nil {
		return *bad
	}
	if len(c.pos) < 1 {
		return failf("missing secret URI")
	}
	id, err := ParseSecretURI(c.pos[0])
	if err != nil {
		return failf("%v", err)
	}
	if len(c.pos) > 1 {
		return failf("unrecognized args: %q", c.pos[1:])
	}
	var rev *int
	if c.has("revision") {
		n, err := parseInt(c.str("revision"))
		if err != nil {
			return failf("invalid value %q for flag --revision: %v", c.str("revision"), err)
		}
		if n > 0 {
			rev = &n
		}
	}
	if err := r.B.RemoveSecret(SecretURI(id), rev); err != nil {
		return failf("%v", err)
	}
	return Response{}
}

func parseInt(s string) (int, error) {
	var n int
	if _, err := fmt.Sscanf(s, "%d", &n); err != nil {
		return 0, errors.New("parse error")
	}
	return n, nil
}

func (r *Real) secretGrant(req Request) Response {
	c, bad := parseReq(flagDef{flags: map[string]bool{"unit": true, "relation": true}, aliases: relationFlag}, req)
	if bad != nil {
		return *bad
	}
	if len(c.pos) < 1 {
		return failf("missing secret URI")
	}
	id, err := ParseSecretURI(c.pos[0])
	if err != nil {
		return failf("%v", err)
	}
	relID, err := r.relationID(c)
	if err != nil {
		return failf("%v", err)
	}
	if relID == -1 {
		return failf("no relation id specified")
	}
	ri, err := r.B.Relation(relID)
	if err != nil {
		return failf("%v", err)
	}
	unit := c.str("unit")
	if unit != "" {
		if !isUnitName(unit) {
			return failf("unit %q not valid", unit)
		}
		if app := unit[:strings.Index(unit, "/")]; app != ri.RemoteApp {
			return failf("cannot specify unit %q in relation to application %q", unit, ri.RemoteApp)
		}
	}
	if len(c.pos) > 1 {
		return failf("unrecognized args: %q", c.pos[1:])
	}
	if err := r.B.GrantSecret(SecretURI(id), SecretGrant{Application: ri.RemoteApp, Unit: unit, Relation: fmt.Sprint(relID)}); err != nil {
		return failf("%v", err)
	}
	return Response{}
}

func (r *Real) secretRevoke(req Request) Response {
	c, bad := parseReq(flagDef{flags: map[string]bool{"app": true, "application": true, "unit": true, "relation": true}, aliases: map[string]string{"r": "relation", "application": "app"}}, req)
	if bad != nil {
		return *bad
	}
	if len(c.pos) < 1 {
		return failf("missing secret URI")
	}
	id, err := ParseSecretURI(c.pos[0])
	if err != nil {
		return failf("%v", err)
	}
	app, unit := c.str("app"), c.str("unit")
	if app != "" && !isAppName(app) {
		return failf("application %q not valid", app)
	}
	if unit != "" && !isUnitName(unit) {
		return failf("unit %q not valid", unit)
	}
	if app != "" && unit != "" {
		return failf("specify only one of application or unit")
	}
	g := SecretGrant{Application: app, Unit: unit}
	if c.has("relation") {
		relID, err := r.relationID(c)
		if err != nil {
			return failf("%v", err)
		}
		ri, err := r.B.Relation(relID)
		if err != nil {
			return failf("%v", err)
		}
		if app != "" {
			return failf("do not specify both relation and app")
		}
		g.Application, g.Relation = ri.RemoteApp, fmt.Sprint(relID)
	}
	if g.Application == "" && g.Unit == "" {
		return failf("missing relation or application or unit")
	}
	if len(c.pos) > 1 {
		return failf("unrecognized args: %q", c.pos[1:])
	}
	if err := r.B.RevokeSecret(SecretURI(id), g); err != nil {
		return failf("%v", err)
	}
	return Response{}
}
