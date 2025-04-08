package txn

import "testing"

// 关于
const zkAddr = "127.0.0.1:2181"
const httpAddr = "127.0.0.1:21820"

func TestNewCoordinator(t *testing.T) {
	// 启动http服务器就ok了

	NewCoordinatorHTTP(zkAddr, httpAddr)
}
