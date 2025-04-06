package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
)

func Test_NodeTxn(t *testing.T) {
	// 加载配置
	config1, err := LoadConfig("./configs/raft_config_1.yaml")
	if err != nil {
		t.Fatalf("Failed to load config1: %v", err)
	}

	// 模拟客户端请求（随机选择一个节点作为入口）
	httpAddr := config1.HttpServerAddr
	id := config1.ID
	t.Logf("Testing node %d at %s", id, httpAddr)

	// --- 辅助函数：发送请求并自动处理 Leader 重定向 ---
	sendRequestToLeader := func(method, url string, body io.Reader) (*http.Response, error) {
		maxRetries := 3 // 最大重试次数
		for i := 0; i < maxRetries; i++ {
			var resp *http.Response
			var err error

			// 根据方法发送请求
			if method == http.MethodGet {
				resp, err = http.Get(url)
			} else {
				resp, err = http.Post(url, "application/json", body)
			}

			if err != nil {
				return nil, err
			}

			// 如果是 Leader 处理成功，直接返回
			if resp.StatusCode == http.StatusOK {
				return resp, nil
			}

			// 如果不是 Leader，解析 Leader ID 并更新 URL
			if resp.StatusCode == http.StatusForbidden {
				leaderBytes, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				leaderID, _ := strconv.ParseUint(string(leaderBytes), 10, 64)

				// 查找 Leader 的 HTTP 地址（这里假设 config 中有所有节点的信息）
				// 实际项目中可能需要从集群配置或服务发现获取
				leaderAddr := getLeaderAddr(leaderID) // 需要实现 getLeaderAddr
				if leaderAddr == "" {
					return nil, fmt.Errorf("failed to find leader address for ID %d", leaderID)
				}

				// 更新 URL，准备重试
				url = fmt.Sprintf("http://%s/raft/%d%s", leaderAddr, leaderID, strings.TrimPrefix(url, fmt.Sprintf("http://%s/raft/%d", httpAddr, id)))
				t.Logf("Redirecting to leader %d at %s", leaderID, leaderAddr)
				continue
			}

			// 其他错误直接返回
			return resp, nil
		}
		return nil, fmt.Errorf("max retries exceeded")
	}

	// --- 测试 1: 发送 PUT 请求 ---
	t.Run("TestTxnPut", func(t *testing.T) {
		kv := map[string]string{"test_key": "test_value"}
		jsonData, _ := json.Marshal(kv)

		url := fmt.Sprintf("http://%s/raft/%d/TxnSet", httpAddr, id)
		resp, err := sendRequestToLeader(http.MethodPost, url, bytes.NewBuffer(jsonData))
		if err != nil {
			t.Fatalf("PUT request failed after retries: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			t.Log("PUT request succeeded (leader)")
		} else {
			t.Errorf("Unexpected status code: %d", resp.StatusCode)
		}
	})

	// --- 测试 2: 发送 DELETE 请求 ---
	t.Run("TestTxnDelete", func(t *testing.T) {
		key := "test_key"
		url := fmt.Sprintf("http://%s/raft/%d/TxnDelete?key=%s", httpAddr, id, key)

		resp, err := sendRequestToLeader(http.MethodPost, url, nil)
		if err != nil {
			t.Fatalf("DELETE request failed after retries: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			t.Log("DELETE request succeeded (leader)")
		} else {
			t.Errorf("Unexpected status code: %d", resp.StatusCode)
		}
	})

	// --- 测试 3: 发送 GET 请求 ---
	t.Run("TestTxnGet", func(t *testing.T) {
		key := "test_key"
		url := fmt.Sprintf("http://%s/raft/%d/TxnGet?key=%s", httpAddr, id, key)

		resp, err := sendRequestToLeader(http.MethodGet, url, nil)
		if err != nil {
			t.Fatalf("GET request failed after retries: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			var value string
			if err := json.NewDecoder(resp.Body).Decode(&value); err != nil {
				t.Errorf("Failed to decode GET response: %v", err)
			}
			t.Logf("GET value: %s", value)
		} else {
			t.Errorf("Unexpected status code: %d", resp.StatusCode)
		}
	})

	// --- 测试 4: 发送 Commit 请求 ---
	t.Run("TestTxnCommit", func(t *testing.T) {
		url := fmt.Sprintf("http://%s/raft/%d/TxnCommit", httpAddr, id)

		resp, err := sendRequestToLeader(http.MethodPost, url, nil)
		if err != nil {
			t.Fatalf("Commit request failed after retries: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			t.Log("Commit succeeded")
		} else {
			t.Errorf("Unexpected status code: %d", resp.StatusCode)
		}
	})
}

// 辅助函数：根据 leaderID 获取 Leader 的 HTTP 地址
func getLeaderAddr(leaderID uint64) string {
	// 这里需要实现从配置或服务发现中查找 Leader 的地址
	// 示例：硬编码或从全局配置中读取
	return "127.0.0.1:8080" // 替换为实际逻辑
}
