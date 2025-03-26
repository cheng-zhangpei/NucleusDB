package ComDB

import (
	"encoding/binary"
	"log"
	"time"
)
import (
	"hash/fnv"
)

// 获取当前的水位线
func getCurrentime() uint64 {
	timestampSec := uint64(time.Now().Unix())
	return timestampSec
}

func generateHashCode(key []byte) uint64 {
	h := fnv.New64a()
	_, err := h.Write(key)
	if err != nil {
		return 0
	}
	return h.Sum64()
}

// Uint64ToBytesBinary 使用 binary 包编码
func Uint64ToBytesBinary(num uint64, order binary.ByteOrder) []byte {
	buf := make([]byte, 8)
	order.PutUint64(buf, num)
	return buf
}

// BytesToUint64Binary 使用 binary 包解码
func BytesToUint64Binary(b []byte, order binary.ByteOrder) uint64 {
	if len(b) != 8 {
		log.Panicf("字节切片长度必须为8，实际为%d", len(b))
	}
	return order.Uint64(b)
}
