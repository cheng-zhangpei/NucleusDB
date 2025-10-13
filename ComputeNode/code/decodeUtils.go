package code

import "ComputeNode/memspace"

// vector decoder

func decodeVector(record VectorRecord) *memspace.MemUint {
	return memspace.NewMemUint([]byte("test"), []byte(""), memspace.Vector)
}
