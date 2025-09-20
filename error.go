package NucleusDB

import "errors"

var (
	ErrKeyIsEmpty             = errors.New("key is empty")
	ErrIndexUpdateFailed      = errors.New("index update failed")
	ErrKeyNotFound            = errors.New("key not found")
	ErrDataFileNotFound       = errors.New("data file not found")
	ErrDataDirectoryCorrupted = errors.New("the directory may be corrupted")
	ErrExceedMaxBatchNum      = errors.New("max batch num exceeded")
	ErrMergeIsProcessing      = errors.New("merge is processing")
	ErrDatabaseIsUsing        = errors.New("other process is using database")
	ErrMergeRatioUnreachable  = errors.New("merge ratio unreachable")
	ErrNoEnoughSpaceForMerge  = errors.New("no enough space for merge ratio")
	ErrMemoryMetaNotFound     = errors.New("memory meta error")
	ErrCompressNumNotEnough   = errors.New("compress num not enough")
	ErrTimestampNotExist      = errors.New("timestamp not exist in the memory")
	ErrEmptyPending           = errors.New("the pending area is empty")
	ErrConflict               = errors.New("the transaction has conflict key!")
	ErrTxnSnapshotSaveFailed  = errors.New("transaction snapshot save failed")
	ErrTxnNotFound            = errors.New("transaction not found")
	ErrDecodeTxnError         = errors.New("decode transaction error")
	ErrGC                     = errors.New("GC ERR!")
)
