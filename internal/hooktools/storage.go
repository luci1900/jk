// SPDX-License-Identifier: AGPL-3.0-only
//
// Derived from juju (https://github.com/juju/juju), AGPL-3.0: storage-get, storage-list and storage-id of
// internal/worker/uniter/runner/jujuc.

package hooktools

import (
	"fmt"
	"strings"
)

func validStorageID(id string) bool {
	name, idx, ok := strings.Cut(id, "/")
	if !ok || name == "" || idx == "" {
		return false
	}
	for _, c := range idx {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// storageGetHelp mimics juju's `storage-get --help`. ops (storage hook events) regex-matches the `-s  (= <id>)`
// line to learn which storage instance the hook is for, so that line's format must not change.
func (r *Real) storageGetHelp() Response {
	id, _ := r.B.HookStorage()
	return Response{Stdout: `Usage: storage-get [options] [-s <storage-id>] [<key>]

Summary:
Prints information for the storage instance with the specified ID.

Options:
--format  (= smart)
    Specify output format (json|smart|yaml)
-o, --output (= "")
    Specify an output file
-s  (= ` + id + `)
    Specifies a storage instance by ID.

Details:
When no ` + "`<key>`" + ` is supplied, all keys values are printed.

` + "`storage-get`" + ` obtains information about storage being attached
to, or detaching from, the unit.

If the executing hook is a storage hook, information about
the storage related to the hook will be reported; this may
be overridden by specifying the name of the storage as reported
by ` + "`storage-list`" + `, and must be specified for non-storage hooks.
`}
}

func (r *Real) storageGet(req Request) Response {
	for _, a := range req.Args {
		if a == "--help" || a == "-h" {
			return r.storageGetHelp()
		}
	}
	c, bad := parseReq(withFormat(map[string]bool{"s": true}, nil), req)
	if bad != nil {
		return *bad
	}
	id, _ := r.B.HookStorage()
	if c.has("s") {
		id = c.str("s")
		if !validStorageID(id) {
			return failf("invalid value %q for flag -s: invalid storage ID %q", id, id)
		}
		if _, err := r.B.Storage(id); err != nil {
			return failf("invalid value %q for flag -s: %v", id, err)
		}
	}
	if id == "" {
		return failf("no storage instance specified")
	}
	if len(c.pos) > 1 {
		return failf("unrecognized args: %q", c.pos[1:])
	}
	info, err := r.B.Storage(id)
	if err != nil {
		return failf("%v", err)
	}
	values := map[string]any{"kind": info.Kind, "location": info.Location}
	if len(c.pos) == 0 || c.pos[0] == "" {
		return write(c, "smart", values)
	}
	if v, ok := values[c.pos[0]]; ok {
		return write(c, "smart", v)
	}
	return failf("%s", fmt.Sprintf("invalid storage attribute %q", c.pos[0]))
}

func (r *Real) storageList(req Request) Response {
	c, bad := parseReq(withFormat(nil, nil), req)
	if bad != nil {
		return *bad
	}
	if len(c.pos) > 1 {
		return failf("unrecognized args: %q", c.pos[1:])
	}
	name := ""
	if len(c.pos) == 1 {
		name = c.pos[0]
	}
	ids := []string{}
	for _, id := range r.B.StorageIDs() {
		if name != "" {
			if n, _, _ := strings.Cut(id, "/"); n != name {
				continue
			}
		}
		ids = append(ids, id)
	}
	return write(c, "smart", ids)
}
