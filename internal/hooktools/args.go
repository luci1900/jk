// SPDX-License-Identifier: AGPL-3.0-only
//
// Derived from juju (https://github.com/juju/juju), AGPL-3.0: the gnuflag/cmd conventions of
// internal/worker/uniter/runner/jujuc (flags may follow positionals, --flag=value, --flag value) and
// cmd/cmd/output.go (the smart, yaml and json formatters).

package hooktools

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"sigs.k8s.io/yaml"
)

// flagDef describes the flags a tool accepts: true takes a value, false is a boolean. Aliases map to a canonical name.
type flagDef struct {
	flags   map[string]bool
	aliases map[string]string
}

type cli struct {
	vals map[string]string
	pos  []string
}

func (c cli) str(name string) string { return c.vals[name] }

// boolean reports a boolean flag: present alone or with =true.
func (c cli) boolean(name string) bool {
	v, ok := c.vals[name]
	if !ok {
		return false
	}
	// gnuflag uses strconv.ParseBool; ops sends Python's `--application=False`.
	b, err := strconv.ParseBool(v)
	return err != nil || b
}

func (c cli) has(name string) bool { _, ok := c.vals[name]; return ok }

// parseCLI parses args the way juju's gnuflag does: flags and positionals may be mixed, `--` ends the flags,
// `-` is a positional, and boolean flags take a value only in the `--flag=value` form.
func parseCLI(def flagDef, args []string) (cli, error) {
	c := cli{vals: map[string]string{}}
	canon := func(n string) string {
		if a, ok := def.aliases[n]; ok {
			return a
		}
		return n
	}
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--":
			c.pos = append(c.pos, args[i+1:]...)
			return c, nil
		case a == "-" || !strings.HasPrefix(a, "-"):
			c.pos = append(c.pos, a)
			continue
		}
		name := strings.TrimLeft(a, "-")
		val, hasVal := "", false
		if k, v, ok := strings.Cut(name, "="); ok {
			name, val, hasVal = k, v, true
		}
		name = canon(name)
		takes, known := def.flags[name]
		if !known {
			return c, fmt.Errorf("flag provided but not defined: %s", strings.SplitN(a, "=", 2)[0])
		}
		switch {
		case takes && hasVal:
			c.vals[name] = val
		case takes:
			if i+1 >= len(args) {
				return c, fmt.Errorf("flag needs an argument: %s", a)
			}
			i++
			c.vals[name] = args[i]
		case hasVal:
			c.vals[name] = val
		default:
			c.vals[name] = "true"
		}
	}
	return c, nil
}

// formatFlags adds the --format flag (and juju's -o/--output is not supported).
func withFormat(flags map[string]bool, aliases map[string]string) flagDef {
	if flags == nil {
		flags = map[string]bool{}
	}
	flags["format"] = true
	return flagDef{flags: flags, aliases: aliases}
}

// write formats value as juju's cmd.Output does and returns the tool's response.
// def is the default formatter ("smart" for most tools).
func write(c cli, def string, value any) Response {
	name := c.str("format")
	if name == "" {
		name = def
	}
	var out string
	switch name {
	case "json":
		b, err := json.Marshal(value)
		if err != nil {
			return failf("%v", err)
		}
		out = string(b) + "\n"
	case "yaml":
		out = formatYAML(value)
	case "smart":
		out = formatSmart(value)
	default:
		return failf("invalid value %q for flag --format: unknown format %q", name, name)
	}
	return Response{Stdout: out}
}

func formatYAML(value any) string {
	if value == nil {
		return ""
	}
	b, err := yaml.Marshal(value)
	if err != nil {
		return ""
	}
	s := strings.TrimRight(string(b), "\n")
	if s == "" {
		return ""
	}
	return s + "\n"
}

// formatSmart is juju's FormatSmart: strings untouched, string lists joined by newlines, booleans as
// True/False (to match pyjuju), nothing for nil or "", anything else as YAML.
func formatSmart(value any) string {
	if value == nil {
		return ""
	}
	var s string
	switch v := value.(type) {
	case string:
		s = v
	case []string:
		s = strings.Join(v, "\n")
	case bool:
		if v {
			s = "True"
		} else {
			s = "False"
		}
	default:
		return formatYAML(value)
	}
	if s == "" {
		return ""
	}
	return s + "\n"
}

func failf(f string, a ...any) Response {
	return Response{Code: 1, Stderr: "ERROR " + fmt.Sprintf(f, a...) + "\n"}
}
