package v1alpha1

import (
	"strconv"
	"strings"
)

// Labels, annotations and names shared by the operator and the unit agents.
const (
	// NamespaceLabel marks cluster-scoped objects with the namespace (model) of the app they belong to, so the
	// Application finalizer can find them (the AppLabel alone is not unique across namespaces).
	NamespaceLabel = Group + "/namespace"
	// PeerEndpointLabel carries the peer endpoint name on the Relation the operator creates for it.
	PeerEndpointLabel = Group + "/peer-endpoint"
	// DestroyStorageAnnotation on an Application tells the finalizer to delete its volumes instead of keeping them.
	DestroyStorageAnnotation = Group + "/destroy-storage"

	// CharmImageAnnotation and CharmRevisionAnnotation are on the pod template: the charm image (digest-pinned
	// reference) and Charmhub revision the pods of that template start with. Pods also get the image as the env
	// var CharmImageEnv; the agent records it in UnitData.spec.charmURL once the unit has run it.
	CharmImageAnnotation    = Group + "/charm-image"
	CharmRevisionAnnotation = Group + "/charm-revision"
	CharmImageEnv           = "JK_CHARM_IMAGE"
	// ForceRemoveAnnotation on a Relation ("true") makes the operator drop its finalizer without waiting for units.
	ForceRemoveAnnotation = Group + "/force-remove"

	// RemoteLabel marks the mirror of a consumer's Relation in the offering namespace; RemoteOfAnnotation names the
	// original as "<namespace>/<name>".
	RemoteLabel        = Group + "/remote"
	RemoteOfAnnotation = Group + "/remote-of"
	// OfferFinalizer on an Offer lets the operator remove its connections first.
	OfferFinalizer = Group + "/offer"
	// OfferAccessConfigMap in SystemNamespace lists "<offering-ns>/<consumer-ns>" pairs (and "<offering-ns>/*") that
	// offers allow; the admission policy of relations across namespaces reads it.
	OfferAccessConfigMap = "jk-offer-access"

	// SecretLabel carries the secret id (the xid of secret:<xid>) on a secret's metadata Secret and all of its
	// revision Secrets. Both also carry AppLabel with the application of the owner (for a unit-owned secret, the
	// unit's application).
	SecretLabel = Group + "/secret"
	// SecretOwnerAnnotation ("application:<app>" or "unit:<unit>") and SecretLatestAnnotation (the latest revision) are
	// on a secret's metadata Secret; the operator rewrites the owner of the copy it makes for another namespace.
	SecretOwnerAnnotation  = Group + "/secret-owner"
	SecretLatestAnnotation = Group + "/secret-latest-revision"
	// SecretMirrorLabel marks the copy of another namespace's secret that the operator keeps for a relation; its value
	// identifies the relation.
	SecretMirrorLabel = Group + "/secret-mirror"
	// SecretRevisionLabel carries the revision number on a revision Secret (the metadata Secret has none).
	SecretRevisionLabel = Group + "/secret-revision"
	// SecretGrantsAnnotation on the metadata Secret is a JSON list of SecretGrant. The operator turns the
	// applications in it into Role rules for their ServiceAccount.
	SecretGrantsAnnotation = Group + "/secret-grants"
	// SecretNamePrefix prefixes the names of both kinds of secret objects.
	SecretNamePrefix = "jk-secret-"
)

// SecretGrant is one entry of the SecretGrantsAnnotation: access for an application (and optionally one of its
// units or a relation, which the agents enforce, as RBAC works per ServiceAccount).
type SecretGrant struct {
	Application string `json:"application"`
	Unit        string `json:"unit,omitempty"`
	Relation    string `json:"relation,omitempty"`
}

// SecretMetadataName is the name of a secret's metadata Secret: jk-secret-<xid>.
func SecretMetadataName(xid string) string { return SecretNamePrefix + xid }

// SecretRevisionName is the name of the Secret holding one revision's content: jk-secret-<xid>-<rev>.
func SecretRevisionName(xid string, rev int) string {
	return SecretNamePrefix + xid + "-" + strconv.Itoa(rev)
}

// PeerRelationName is the name of the peer Relation of an application endpoint: <app>.<endpoint>, with
// underscores in the endpoint turned into hyphens (object names cannot contain them).
func PeerRelationName(app, endpoint string) string {
	return app + "." + strings.ReplaceAll(endpoint, "_", "-")
}
