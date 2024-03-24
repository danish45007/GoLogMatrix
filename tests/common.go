package tests

type OperationType int

const (
	// InsertionOperation is an operation type for inserting data into key-value store.
	InsertionOperation OperationType = iota
	// DeletionOperation is an operation type for deleting data from key-value store.
	DeletionOperation
)

type Record struct {
	Op    OperationType `json:"op"`    // Operation type
	Key   string        `json:"key"`   // Key
	Value []byte        `json:"value"` // Value
}
