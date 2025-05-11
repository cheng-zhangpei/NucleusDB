package txn_client

import (
	"ComDB/raft"
	"fmt"
	"github.com/stretchr/testify/assert"
	"log"
	"net/http"
	"testing"
)

// 测试事务客户端
func TestTxnClient_Update(t *testing.T) {
	// 创建客户端
	client := NewTxnClient("../configs/raft_config_1.yaml")
	// 调用 update 提交事务
	results, err := client.update(func(tc *txnClient) error {
		// 在这里写你要执行的事务操作
		if err := tc.Put("key1", "value1"); err != nil {
			return err
		}
		if err := tc.Put("key2", "value2"); err != nil {
			return err
		}
		if err := tc.Put("key4", "value2"); err != nil {
			return err
		}
		if err := tc.Put("key5", "value2"); err != nil {
			return err
		}
		if err := tc.Get("key1"); err != nil {
			return err
		}
		if err := tc.Get("key2"); err != nil {
			return err
		}
		if err := tc.Delete("key2"); err != nil {
			return err
		}
		return nil
	})
	// 断言无错误
	assert.NoError(t, err)
	if len(results) == 0 {

	}
	// 打印结果
	for i, val := range results {
		t.Logf("Result[%d]: %s\n", i, val)
	}
}

func TestRaftGet(t *testing.T) {
	// 模拟客户端请求
	key := "key5"
	config1, err := raft.LoadConfig("../configs/raft_config_1.yaml")
	if err != nil {
		panic(err)
	}
	config2, err := raft.LoadConfig("../configs/raft_config_2.yaml")
	if err != nil {
		panic(err)
	}
	config3, err := raft.LoadConfig("../configs/raft_config_3.yaml")
	if err != nil {
		panic(err)
	}
	configs := []*raft.RaftConfig{config1, config2, config3}

	for _, config := range configs {
		httpAddr := config.HttpServerAddr
		id := config.ID
		// 构造初始请求 URL
		getEndpoint := fmt.Sprintf("http://%s/raft/%d/get?key=%s", httpAddr, id, key)
		var resp *http.Response
		// 发送 GET 请求
		resp, _ = http.Get(getEndpoint)
		log.Println(resp)
	}
}
