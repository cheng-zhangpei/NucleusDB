package transaction

import "time"

// 获取当前的水位线
func getStartTime() uint64 {
	timestampSec := uint64(time.Now().Unix())
	return timestampSec
}
