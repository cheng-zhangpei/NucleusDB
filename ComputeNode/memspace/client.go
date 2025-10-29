package memspace

type Client interface {
	// Update :transaction operation in the database
	Update(fn func(TxnOperation) error) ([]string, error)
	Commit() error
}

// TxnOperation interface for TxnOperation
type TxnOperation interface {
	// TxnGet :get value from the distributed database
	TxnGet([]byte) error
	// TxnPut :set value into distributed database
	TxnPut([]byte, []byte) error
	// TxnDelete :Delete data in a distributed database
	TxnDelete([]byte) error
}

// DistributeOperation interface for distributed database operation
type DistributeOperation interface {
	DistributeGet([]byte) ([]byte, error)
	DistributePut([]byte, []byte) error
	DistributeDelete([]byte) error
	DistributePrefixList([]byte) ([][]byte, error)
}

// StandAloneOperation interface for single node database
// todo 暂时不想做,偷懒的czp
type StandAloneOperation interface {
	StandAloneGet([]byte) ([]byte, error)
	StandAlonePut([]byte, []byte) error
	StandAloneDelete([]byte) error
}
