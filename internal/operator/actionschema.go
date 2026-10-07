package operator

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"
)

// ActionSchema builds the JSON schema of one action from its entry in actions.yaml, the way juju does
// (domain/deployment/charm/actions.go): `params` are the properties, `required` and every other schema key are kept,
// and unknown properties are rejected unless the entry says otherwise.
func ActionSchema(name string, entry map[string]any) map[string]any {
	schema := map[string]any{"type": "object", "title": name, "properties": map[string]any{}, "additionalProperties": false}
	for k, v := range entry {
		switch k {
		case "params":
			schema["properties"] = v
		case "parallel", "execution-group":
		default:
			schema[k] = v
		}
	}
	return schema
}

// execSchema is the schema of the predefined juju-exec action. juju requires `timeout` too; here the Action's
// timeoutSeconds carries it, so only `command` is required.
var execSchema = map[string]any{
	"type": "object", "title": "juju-exec",
	"required": []any{"command"},
	"properties": map[string]any{
		"command":          map[string]any{"type": "string", "minLength": float64(1)},
		"timeout":          map[string]any{"type": "number"},
		"workload-context": map[string]any{"type": "boolean"},
	},
}

// ActionSchemas maps the charm's action names to their schemas from the actions.yaml in status.charm, plus the
// predefined juju-exec.
func ActionSchemas(actionsJSON []byte) (map[string]map[string]any, error) {
	out := map[string]map[string]any{"juju-exec": execSchema}
	if len(actionsJSON) == 0 {
		return out, nil
	}
	var raw map[string]map[string]any
	if err := json.Unmarshal(actionsJSON, &raw); err != nil {
		return nil, fmt.Errorf("parsing the charm's actions: %w", err)
	}
	for name, entry := range raw {
		if name == "juju-exec" {
			continue
		}
		out[name] = ActionSchema(name, entry)
	}
	return out, nil
}

// ValidateActionParams validates params against an action schema and returns every problem found, sorted.
func ValidateActionParams(schema map[string]any, params map[string]any) []string {
	if params == nil {
		params = map[string]any{}
	}
	var errs []string
	validateValue(schema, params, "(root)", &errs)
	sort.Strings(errs)
	return errs
}

func validateValue(schema map[string]any, v any, path string, errs *[]string) {
	add := func(format string, args ...any) { *errs = append(*errs, path+": "+fmt.Sprintf(format, args...)) }
	if t, ok := schema["type"]; ok && !typeMatches(t, v) {
		add("invalid type, expected %s, given %s", typeString(t), jsonType(v))
		return
	}
	if enum, ok := schema["enum"].([]any); ok {
		found := false
		for _, e := range enum {
			if reflect.DeepEqual(e, v) {
				found = true
			}
		}
		if !found {
			add("must be one of the following: %s", joinJSON(enum))
		}
	}
	for _, sub := range subSchemas(schema["allOf"]) {
		validateValue(sub, v, path, errs)
	}
	if anyOf := subSchemas(schema["anyOf"]); len(anyOf) > 0 && !anyValid(anyOf, v) {
		add("must match at least one of the anyOf schemas")
	}
	if one := subSchemas(schema["oneOf"]); len(one) > 0 {
		n := 0
		for _, sub := range one {
			if len(check(sub, v)) == 0 {
				n++
			}
		}
		if n != 1 {
			add("must match exactly one of the oneOf schemas, matches %d", n)
		}
	}
	if not, ok := schema["not"].(map[string]any); ok && len(check(not, v)) == 0 {
		add("must not match the not schema")
	}
	switch x := v.(type) {
	case string:
		n := utf8.RuneCountInString(x)
		if m, ok := num(schema["minLength"]); ok && float64(n) < m {
			add("string length must be greater than or equal to %v", m)
		}
		if m, ok := num(schema["maxLength"]); ok && float64(n) > m {
			add("string length must be less than or equal to %v", m)
		}
		if p, ok := schema["pattern"].(string); ok {
			if re, err := regexp.Compile(p); err != nil {
				add("invalid pattern %q in the charm's action schema", p)
			} else if !re.MatchString(x) {
				add("does not match pattern '%s'", p)
			}
		}
	case float64:
		validateNumber(schema, x, add)
	case []any:
		if m, ok := num(schema["minItems"]); ok && float64(len(x)) < m {
			add("array must have at least %v items", m)
		}
		if m, ok := num(schema["maxItems"]); ok && float64(len(x)) > m {
			add("array must have at most %v items", m)
		}
		if u, _ := schema["uniqueItems"].(bool); u {
			for i := range x {
				for j := i + 1; j < len(x); j++ {
					if reflect.DeepEqual(x[i], x[j]) {
						add("items %d and %d must be unique", i, j)
					}
				}
			}
		}
		switch items := schema["items"].(type) {
		case map[string]any:
			for i, e := range x {
				validateValue(items, e, fmt.Sprintf("%s.%d", path, i), errs)
			}
		case []any:
			for i, e := range x {
				if i < len(items) {
					if sub, ok := items[i].(map[string]any); ok {
						validateValue(sub, e, fmt.Sprintf("%s.%d", path, i), errs)
					}
				}
			}
		}
	case map[string]any:
		validateObject(schema, x, path, errs, add)
	}
}

func validateNumber(schema map[string]any, x float64, add func(string, ...any)) {
	if m, ok := num(schema["minimum"]); ok {
		excl, _ := schema["exclusiveMinimum"].(bool)
		if x < m || (excl && x == m) {
			add("must be greater than %s%v", orEqual(excl), m)
		}
	}
	if m, ok := num(schema["maximum"]); ok {
		excl, _ := schema["exclusiveMaximum"].(bool)
		if x > m || (excl && x == m) {
			add("must be less than %s%v", orEqual(excl), m)
		}
	}
	if m, ok := schema["exclusiveMinimum"].(float64); ok && x <= m {
		add("must be greater than %v", m)
	}
	if m, ok := schema["exclusiveMaximum"].(float64); ok && x >= m {
		add("must be less than %v", m)
	}
	if m, ok := num(schema["multipleOf"]); ok && m > 0 && math.Abs(x/m-math.Round(x/m)) > 1e-9 {
		add("must be a multiple of %v", m)
	}
}

func orEqual(exclusive bool) string {
	if exclusive {
		return ""
	}
	return "or equal to "
}

func validateObject(schema map[string]any, x map[string]any, path string, errs *[]string, add func(string, ...any)) {
	if req, ok := schema["required"].([]any); ok {
		for _, r := range req {
			if name, ok := r.(string); ok {
				if _, present := x[name]; !present {
					*errs = append(*errs, fmt.Sprintf("%s: %s is required", path, name))
				}
			}
		}
	}
	if m, ok := num(schema["minProperties"]); ok && float64(len(x)) < m {
		add("must have at least %v properties", m)
	}
	if m, ok := num(schema["maxProperties"]); ok && float64(len(x)) > m {
		add("must have at most %v properties", m)
	}
	props, _ := schema["properties"].(map[string]any)
	patterns, _ := schema["patternProperties"].(map[string]any)
	for _, k := range sortedKeys(x) {
		val := x[k]
		matched := false
		if sub, ok := props[k].(map[string]any); ok {
			matched = true
			validateValue(sub, val, path+"."+k, errs)
		} else if _, ok := props[k]; ok {
			matched = true
		}
		for p, sub := range patterns {
			if re, err := regexp.Compile(p); err == nil && re.MatchString(k) {
				matched = true
				if s, ok := sub.(map[string]any); ok {
					validateValue(s, val, path+"."+k, errs)
				}
			}
		}
		if matched {
			continue
		}
		switch ap := schema["additionalProperties"].(type) {
		case bool:
			if !ap {
				*errs = append(*errs, fmt.Sprintf("%s: additional property %s is not allowed", path, k))
			}
		case map[string]any:
			validateValue(ap, val, path+"."+k, errs)
		}
	}
}

func check(schema map[string]any, v any) []string {
	var errs []string
	validateValue(schema, v, "", &errs)
	return errs
}

func anyValid(schemas []map[string]any, v any) bool {
	for _, s := range schemas {
		if len(check(s, v)) == 0 {
			return true
		}
	}
	return false
}

func subSchemas(v any) []map[string]any {
	list, _ := v.([]any)
	var out []map[string]any
	for _, e := range list {
		if m, ok := e.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out
}

func num(v any) (float64, bool) {
	f, ok := v.(float64)
	return f, ok
}

func jsonType(v any) string {
	switch x := v.(type) {
	case nil:
		return "null"
	case bool:
		return "boolean"
	case float64:
		if x == math.Trunc(x) {
			return "integer"
		}
		return "number"
	case string:
		return "string"
	case []any:
		return "array"
	case map[string]any:
		return "object"
	}
	return fmt.Sprintf("%T", v)
}

func typeMatches(t any, v any) bool {
	var types []string
	switch x := t.(type) {
	case string:
		types = []string{x}
	case []any:
		for _, e := range x {
			if s, ok := e.(string); ok {
				types = append(types, s)
			}
		}
	}
	got := jsonType(v)
	for _, want := range types {
		if want == got || (want == "number" && got == "integer") {
			return true
		}
	}
	return false
}

func typeString(t any) string {
	if s, ok := t.(string); ok {
		return s
	}
	var parts []string
	if l, ok := t.([]any); ok {
		for _, e := range l {
			parts = append(parts, fmt.Sprint(e))
		}
	}
	return strings.Join(parts, " or ")
}

func joinJSON(list []any) string {
	parts := make([]string, len(list))
	for i, e := range list {
		b, _ := json.Marshal(e)
		parts[i] = string(b)
	}
	return strings.Join(parts, ", ")
}

// InsertActionDefaults fills the `default` of every property of the schema that params does not set, recursing into
// object-valued parameters that are present. It returns a new map; params is not changed.
func InsertActionDefaults(schema map[string]any, params map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range params {
		out[k] = v
	}
	props, _ := schema["properties"].(map[string]any)
	for name, p := range props {
		sub, ok := p.(map[string]any)
		if !ok {
			continue
		}
		cur, present := out[name]
		switch {
		case !present:
			if d, ok := sub["default"]; ok {
				out[name] = deepCopyJSON(d)
			}
		default:
			if obj, ok := cur.(map[string]any); ok {
				out[name] = InsertActionDefaults(sub, obj)
			}
		}
	}
	return out
}

func deepCopyJSON(v any) any {
	switch x := v.(type) {
	case map[string]any:
		m := make(map[string]any, len(x))
		for k, e := range x {
			m[k] = deepCopyJSON(e)
		}
		return m
	case []any:
		l := make([]any, len(x))
		for i, e := range x {
			l[i] = deepCopyJSON(e)
		}
		return l
	}
	return v
}
