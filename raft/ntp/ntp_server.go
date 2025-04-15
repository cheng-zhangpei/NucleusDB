package ntp

import (
	"time"
)

// NTPService 提供统一接口
type NTPService interface {
	Now() (time.Time, error)      // 获取校准后的时间
	Sync() (time.Duration, error) // 主动同步一次时间
	Offset() time.Duration        // 当前本地与服务器时间偏差
}
