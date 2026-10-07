// SPDX-License-Identifier: AGPL-3.0-only
//
// Derived from juju (https://github.com/juju/juju), AGPL-3.0: the contexts of internal/worker/uniter/runner/jujuc
// (relation, secret, storage, network and goal-state) that the tools below use.

package hooktools

import (
	"errors"
	"fmt"
	"time"
)

// Errors a Backend returns; the tools print their messages, which charm libraries match on ("not found", "permission denied").
var (
	// ErrNotFound wraps every "X not found" error.
	ErrNotFound = errors.New("not found")
	// ErrNotLeader is returned by writes that only the leader may make.
	ErrNotLeader = errors.New("this unit is not the leader")
	// ErrPermission is returned for secrets the unit may not read or change.
	ErrPermission = errors.New("permission denied")
)

// NotFoundf returns an error satisfying errors.Is(err, ErrNotFound) that reads "<what> not found".
func NotFoundf(format string, args ...any) error {
	return fmt.Errorf("%s %w", fmt.Sprintf(format, args...), ErrNotFound)
}

// HookRelation describes the relation of the executing relation hook.
type HookRelation struct {
	ID         int
	Endpoint   string
	RemoteUnit string // empty in application, created and broken hooks
	RemoteApp  string
}

// RelationInfo is a relation as the unit sees it.
type RelationInfo struct {
	ID        int
	Endpoint  string
	RemoteApp string
	// Units are the remote units joined so far, sorted.
	Units []string
}

// RelationBackend is the relation side of a hook context.
type RelationBackend interface {
	// HookRelation returns the relation of a relation hook (ok is false in other hooks).
	HookRelation() (HookRelation, bool)
	// RelationIDs lists the relations the unit is in, except broken ones, sorted.
	RelationIDs() []int
	// Relation returns a relation or an error satisfying ErrNotFound.
	Relation(id int) (RelationInfo, error)
	// ReadRelationSettings reads the settings of a remote or local unit, or (app) of an application, in a relation.
	// A unit's own settings (and the application's, for the leader) include the changes made in this hook.
	ReadRelationSettings(id int, name string, app bool) (map[string]string, error)
	// SetRelationSettings changes the unit's (or, for the leader, the application's) settings; an empty value removes the key.
	SetRelationSettings(id int, app bool, kv map[string]string) error
	ModelUUID() string
}

// SecretOwner kinds.
const (
	OwnerApplication = "application"
	OwnerUnit        = "unit"
)

// SecretAccess is a grant shown by secret-info-get.
type SecretAccess struct {
	Target string `json:"target" yaml:"target"`
	Scope  string `json:"scope" yaml:"scope"`
	Role   string `json:"role" yaml:"role"`
}

// SecretMetadata describes a secret owned by the unit or its application (juju's jujuc.SecretMetadata).
type SecretMetadata struct {
	Description    string
	Label          string
	Owner          string // OwnerApplication or OwnerUnit
	RotatePolicy   string
	LatestRevision int
	Expire         *time.Time
	Access         []SecretAccess
}

// SecretCreate is the content of secret-add.
type SecretCreate struct {
	Owner        string
	Label        string
	Description  string
	RotatePolicy string
	Expire       *time.Time
	// Content holds the decoded values.
	Content map[string][]byte
}

// SecretUpdate is the content of secret-set: nil fields are unchanged.
type SecretUpdate struct {
	Label        *string
	Description  *string
	RotatePolicy *string
	Expire       *time.Time
	// Content, when non-empty, becomes a new revision.
	Content map[string][]byte
}

// SecretGrant names who gets (or loses) access to a secret.
type SecretGrant struct {
	// Application and Unit are the grantee; Relation is the relation id the grant belongs to ("" for none).
	Application string
	Unit        string
	Relation    string
}

// SecretBackend is the secrets side of a hook context. Writes are buffered by the agent and committed with the hook.
type SecretBackend interface {
	// CreateSecret returns the new secret's URI (secret:<xid>).
	CreateSecret(SecretCreate) (string, error)
	UpdateSecret(uri string, upd SecretUpdate) error
	// RemoveSecret removes a revision, or all of the secret when revision is nil.
	RemoveSecret(uri string, revision *int) error
	// GetSecret returns the content of a secret by URI, by owner label or by the unit's consumer label.
	GetSecret(uri, label string, refresh, peek bool) (map[string][]byte, error)
	// SecretMetadata lists the secrets owned by the unit or application, by secret id (the xid), including this hook's changes.
	SecretMetadata() (map[string]SecretMetadata, error)
	GrantSecret(uri string, g SecretGrant) error
	RevokeSecret(uri string, g SecretGrant) error
}

// StorageInfo is one storage instance.
type StorageInfo struct {
	ID       string // name/index
	Kind     string // filesystem
	Location string
}

// StorageBackend is the storage side of a hook context.
type StorageBackend interface {
	// HookStorage returns the storage id of a storage hook.
	HookStorage() (string, bool)
	// StorageIDs lists the unit's storage instances, sorted.
	StorageIDs() []string
	Storage(id string) (StorageInfo, error)
}

// NetworkInfo is the unit's network as the agent sees it.
type NetworkInfo struct {
	Interface string
	MAC       string
	Address   string
	// CIDR is the address with its prefix length.
	CIDR string
}

// GoalStatus is one unit's or relation's goal state.
type GoalStatus struct {
	Status string
	Since  time.Time
}

// GoalState is what goal-state prints.
type GoalState struct {
	Units     map[string]GoalStatus
	Relations map[string]map[string]GoalStatus
}

// ClusterBackend is the part of the context that looks at the world outside the unit.
type ClusterBackend interface {
	// NetworkInfo is the unit's network info; an empty Address means unknown.
	NetworkInfo() NetworkInfo
	// BindingNames are the endpoint and extra-binding names of the charm.
	BindingNames() []string
	GoalState() (GoalState, error)
}
