package v1alpha1

const (
	// ModelLabel marks a namespace as a jk model; jk only acts in marked namespaces.
	ModelLabel = Group + "/model"
	// AppLabel carries the owning application's name on objects the operator or charms create.
	AppLabel = Group + "/app"
	// OperationLabel carries an operation id on Actions.
	OperationLabel = Group + "/operation"
	// ModelConfigMap holds model config and the id counters.
	ModelConfigMap = "jk-model"
	// SystemNamespace hosts the operator and the registry.
	SystemNamespace = "jk-system"
)
