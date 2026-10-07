// SPDX-License-Identifier: AGPL-3.0-only

package hooktools

import "testing"

func storageReal(t *testing.T) (*Real, *fakeBackend) {
	r, f := newReal(t)
	f.storage["pgdata/0"] = StorageInfo{ID: "pgdata/0", Kind: "filesystem", Location: "/var/lib/juju/storage/pgdata/0"}
	f.storage["logs/3"] = StorageInfo{ID: "logs/3", Kind: "filesystem", Location: "/var/lib/juju/storage/logs/0"}
	return r, f
}

func TestStorageList(t *testing.T) {
	r, f := storageReal(t)
	tests := []struct {
		args []string
		want string
	}{
		{nil, "logs/3\npgdata/0\n"},
		{[]string{"pgdata"}, "pgdata/0\n"},
		{[]string{"--format=json"}, `["logs/3","pgdata/0"]` + "\n"},
		{[]string{"nope", "--format=json"}, "[]\n"},
		{[]string{"nope"}, ""},
	}
	for _, tt := range tests {
		if got := rcall(r, "", "storage-list", tt.args...); got.Code != 0 || got.Stdout != tt.want {
			t.Errorf("storage-list %v = %+v, want %q", tt.args, got, tt.want)
		}
	}
	if rcall(r, "", "storage-list", "a", "b").Code == 0 {
		t.Error("extra args accepted")
	}
	f.storage = map[string]StorageInfo{}
	if got := rcall(r, "", "storage-list", "--format=json"); got.Stdout != "[]\n" {
		t.Errorf("%+v", got)
	}
}

func TestStorageGet(t *testing.T) {
	r, f := storageReal(t)
	tests := []struct {
		name string
		args []string
		code int
		want string
	}{
		{"all by id", []string{"-s", "pgdata/0", "--format=json"}, 0, `{"kind":"filesystem","location":"/var/lib/juju/storage/pgdata/0"}` + "\n"},
		{"yaml", []string{"-s", "pgdata/0"}, 0, "kind: filesystem\nlocation: /var/lib/juju/storage/pgdata/0\n"},
		{"flag after the key, as ops sends it", []string{"location", "-s", "logs/3"}, 0, "/var/lib/juju/storage/logs/0\n"},
		{"kind", []string{"-s", "pgdata/0", "kind"}, 0, "filesystem\n"},
		{"unknown key", []string{"-s", "pgdata/0", "nope"}, 1, ""},
		{"unknown storage", []string{"-s", "x/1"}, 1, ""},
		{"invalid id", []string{"-s", "pgdata"}, 1, ""},
		{"no storage", []string{"location"}, 1, ""},
		{"extra", []string{"-s", "pgdata/0", "kind", "x"}, 1, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := rcall(r, "", "storage-get", tt.args...)
			if got.Code != tt.code || (tt.code == 0 && got.Stdout != tt.want) {
				t.Fatalf("%+v, want %q", got, tt.want)
			}
		})
	}
	// In a storage hook the hook's storage is the default.
	f.hookStor = "pgdata/0"
	if got := rcall(r, "", "storage-get", "location"); got.Stdout != "/var/lib/juju/storage/pgdata/0\n" {
		t.Errorf("%+v", got)
	}
	if got := rcall(r, "", "storage-get", "-s", "logs/3", "kind"); got.Stdout != "filesystem\n" {
		t.Errorf("%+v", got)
	}
}
