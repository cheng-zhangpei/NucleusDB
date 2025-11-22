package NucleusCN

import "errors"

var (
	ErrKeyNotFound      = errors.New("key can not be empty")
	ErrPrefixList       = errors.New("prefix key can no find")
	ErrMetaNotExist     = errors.New("memspace meta can no find")
	ErrMetaSpaceType    = errors.New(" meta memspaceType is empty")
	ErrMemSpaceNotExist = errors.New("memspace can no find")
)
