package operator

import (
	"encoding/json"
	"strings"
	"testing"
)

func j(t *testing.T, s string) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		t.Fatal(err)
	}
	return m
}

const snapshotActions = `{
 "snapshot": {"description": "d", "params": {
    "filename": {"type": "string", "default": "out.tar", "minLength": 3, "pattern": "^[a-z.]+$"},
    "compression": {"type": "object", "properties": {"kind": {"type": "string", "enum": ["gz", "xz"], "default": "gz"}, "quality": {"type": "integer", "minimum": 1, "maximum": 9}}, "required": ["kind"]},
    "tags": {"type": "array", "items": {"type": "string"}, "minItems": 1, "maxItems": 2, "uniqueItems": true},
    "ratio": {"type": "number", "exclusiveMinimum": true, "minimum": 0, "multipleOf": 0.5},
    "flag": {"type": ["boolean", "null"]},
    "any": {"anyOf": [{"type": "string"}, {"type": "integer"}]},
    "one": {"oneOf": [{"type": "string"}, {"type": "integer"}]},
    "not": {"not": {"type": "string"}},
    "all": {"allOf": [{"type": "string"}, {"maxLength": 2}]},
    "tuple": {"type": "array", "items": [{"type": "string"}, {"type": "integer"}]},
    "dyn": {"type": "object", "patternProperties": {"^x-": {"type": "integer"}}, "additionalProperties": {"type": "string"}, "minProperties": 1, "maxProperties": 3}
   }, "required": ["filename"]},
 "open": {"params": {}, "additionalProperties": true},
 "noparams": {"description": "x", "parallel": true, "execution-group": "g"}
}`

func TestValidateActionParams(t *testing.T) {
	schemas, err := ActionSchemas([]byte(snapshotActions))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name, action, params string
		want                 string // substring of the joined errors; "" means valid
	}{
		{"valid minimal", "snapshot", `{"filename":"a.tar"}`, ""},
		{"valid full", "snapshot", `{"filename":"a.tar","compression":{"kind":"xz","quality":3},"tags":["a"],"ratio":1.5,"flag":null,"any":3,"one":1,"not":1,"all":"ab","tuple":["a",1],"dyn":{"x-a":1,"b":"c"}}`, ""},
		{"required missing", "snapshot", `{}`, "filename is required"},
		{"wrong type", "snapshot", `{"filename":3}`, "invalid type, expected string, given integer"},
		{"unknown property", "snapshot", `{"filename":"abc","nope":1}`, "additional property nope is not allowed"},
		{"too short", "snapshot", `{"filename":"a"}`, "string length must be greater than or equal to 3"},
		{"pattern", "snapshot", `{"filename":"ABC"}`, "does not match pattern"},
		{"nested required", "snapshot", `{"filename":"abc","compression":{}}`, "kind is required"},
		{"nested enum", "snapshot", `{"filename":"abc","compression":{"kind":"zip"}}`, `must be one of the following: "gz", "xz"`},
		{"minimum", "snapshot", `{"filename":"abc","compression":{"kind":"gz","quality":0}}`, "must be greater than or equal to 1"},
		{"maximum", "snapshot", `{"filename":"abc","compression":{"kind":"gz","quality":10}}`, "must be less than or equal to 9"},
		{"integer given float", "snapshot", `{"filename":"abc","compression":{"kind":"gz","quality":2.5}}`, "expected integer, given number"},
		{"array too long", "snapshot", `{"filename":"abc","tags":["a","b","c"]}`, "at most 2"},
		{"array too short", "snapshot", `{"filename":"abc","tags":[]}`, "at least 1"},
		{"array not unique", "snapshot", `{"filename":"abc","tags":["a","a"]}`, "must be unique"},
		{"array item type", "snapshot", `{"filename":"abc","tags":[1]}`, "expected string"},
		{"exclusive minimum", "snapshot", `{"filename":"abc","ratio":0}`, "must be greater than 0"},
		{"multiple of", "snapshot", `{"filename":"abc","ratio":0.7}`, "multiple of 0.5"},
		{"type list", "snapshot", `{"filename":"abc","flag":"x"}`, "boolean or null"},
		{"anyOf", "snapshot", `{"filename":"abc","any":true}`, "anyOf"},
		{"oneOf none match", "snapshot", `{"filename":"abc","one":true}`, "exactly one"},
		{"not", "snapshot", `{"filename":"abc","not":"s"}`, "not schema"},
		{"allOf", "snapshot", `{"filename":"abc","all":"abc"}`, "less than or equal to 2"},
		{"tuple", "snapshot", `{"filename":"abc","tuple":[1,1]}`, "expected string"},
		{"pattern properties", "snapshot", `{"filename":"abc","dyn":{"x-a":"s"}}`, "expected integer"},
		{"additional schema", "snapshot", `{"filename":"abc","dyn":{"b":1}}`, "expected string"},
		{"min properties", "snapshot", `{"filename":"abc","dyn":{}}`, "at least 1 properties"},
		{"max properties", "snapshot", `{"filename":"abc","dyn":{"a":"1","b":"1","c":"1","d":"1"}}`, "at most 3 properties"},
		{"open schema", "open", `{"anything":[1,2]}`, ""},
		{"no params, none given", "noparams", `{}`, ""},
		{"no params, one given", "noparams", `{"a":1}`, "not allowed"},
		{"exec valid", "juju-exec", `{"command":"ls","timeout":5,"workload-context":true}`, ""},
		{"exec needs a command", "juju-exec", `{}`, "command is required"},
		{"exec empty command", "juju-exec", `{"command":""}`, "greater than or equal to 1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := strings.Join(ValidateActionParams(schemas[tt.action], j(t, tt.params)), "; ")
			if tt.want == "" && errs != "" || !strings.Contains(errs, tt.want) {
				t.Errorf("errors %q, want %q", errs, tt.want)
			}
		})
	}
	if errs := ValidateActionParams(schemas["noparams"], nil); len(errs) != 0 {
		t.Errorf("nil params: %v", errs)
	}
}

func TestInsertActionDefaults(t *testing.T) {
	schemas, _ := ActionSchemas([]byte(snapshotActions))
	in := j(t, `{"compression":{"quality":2}}`)
	got := InsertActionDefaults(schemas["snapshot"], in)
	b, _ := json.Marshal(got)
	if string(b) != `{"compression":{"kind":"gz","quality":2},"filename":"out.tar"}` {
		t.Errorf("%s", b)
	}
	if _, ok := in["filename"]; ok {
		t.Error("input modified")
	}
	// Defaults are copies: changing one result does not change the schema.
	s := map[string]any{"properties": map[string]any{"l": map[string]any{"default": []any{"a"}}}}
	a, b2 := InsertActionDefaults(s, nil), InsertActionDefaults(s, nil)
	a["l"].([]any)[0] = "z"
	if b2["l"].([]any)[0] != "a" {
		t.Error("default shared between results")
	}
}

func TestActionSchemasErrors(t *testing.T) {
	if s, err := ActionSchemas(nil); err != nil || len(s) != 1 {
		t.Errorf("%v %v", s, err)
	}
	if _, err := ActionSchemas([]byte(`[1]`)); err == nil {
		t.Error("a list is not actions.yaml")
	}
	s, _ := ActionSchemas([]byte(`{"juju-exec":{"params":{"x":{}}}}`))
	if _, ok := s["juju-exec"]["properties"].(map[string]any)["command"]; !ok {
		t.Error("the predefined juju-exec must not be overridden by the charm")
	}
	s, _ = ActionSchemas([]byte(`{"a":{"params":{"x":{"pattern":"("}}}}`))
	if errs := ValidateActionParams(s["a"], map[string]any{"x": "s"}); len(errs) != 1 || !strings.Contains(errs[0], "invalid pattern") {
		t.Errorf("%v", errs)
	}
}
