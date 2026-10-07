package hooktools

import (
	"regexp"
	"testing"
)

// The regex ops uses in _storage_event_details (ops/model.py, _STORAGE_KEY_RE).
var opsStorageKeyRE = regexp.MustCompile(`(?ms).*^-s\s+\(=\s+(.*?)\)\s*?$`)

func TestStorageGetHelpHasKeyLineForOps(t *testing.T) {
	r, f := newReal(t)
	f.hookStor = "data/0"
	for _, flag := range []string{"--help", "-h"} {
		got := rcall(r, "", "storage-get", flag)
		if got.Code != 0 {
			t.Fatalf("%s: %+v", flag, got)
		}
		m := opsStorageKeyRE.FindStringSubmatch(got.Stdout)
		if m == nil || m[1] != "data/0" {
			t.Fatalf("ops regex does not find the storage id in help output:\n%s", got.Stdout)
		}
	}
}
