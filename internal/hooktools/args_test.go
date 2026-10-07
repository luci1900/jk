package hooktools

import "testing"

// ops (Python) sends `--application=False`; gnuflag's strconv.ParseBool accepts every capitalisation.
func TestBooleanFlagValues(t *testing.T) {
	for v, want := range map[string]bool{"true": true, "True": true, "TRUE": true, "1": true, "t": true, "T": true,
		"false": false, "False": false, "FALSE": false, "0": false, "f": false, "F": false} {
		c := cli{vals: map[string]string{"application": v}}
		if got := c.boolean("application"); got != want {
			t.Errorf("--application=%s: got %v want %v", v, got, want)
		}
	}
	if (cli{vals: map[string]string{}}).boolean("application") {
		t.Error("unset flag must be false")
	}
}
