package agent

import "testing"

// A storage with a declared location is mounted there in the charm container (juju's behaviour); without one it is
// mounted under the data dir.
func TestStorageDirHonoursDeclaredLocation(t *testing.T) {
	a := &Agent{cfg: Config{DataDir: "/var/lib/juju"}, charm: &Charm{Storage: map[string]StorageDef{
		"pgdata":  {Type: "filesystem", Location: "/var/lib/postgresql/data"},
		"scratch": {Type: "filesystem"},
	}}}
	if got := a.storageDir("pgdata"); got != "/var/lib/postgresql/data" {
		t.Errorf("pgdata: %s", got)
	}
	if got := a.storageDir("scratch"); got != "/var/lib/juju/storage/scratch/0" {
		t.Errorf("scratch: %s", got)
	}
	if got := a.storageDir("unknown"); got != "/var/lib/juju/storage/unknown/0" {
		t.Errorf("unknown: %s", got)
	}
}
