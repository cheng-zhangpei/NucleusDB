package ComputeNode

import "errors"

var (
	ErrKeyNotFound = errors.New("key can not be empty")
	ErrPrefixList  = errors.New("prefix key can no find")
)
