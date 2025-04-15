package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
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
	log.Println("send to : " + httpAddr)
	id := config1.ID
	t.Logf("Testing node %d at %s", id, httpAddr)
	// --- 辅助函数：发送请求并自动处理 Leader 重定向 ---
	sendRequestToLeader := func(method, url string, body io.Reader) (*http.Response, error) {
		maxRetries := 3
		var bodyBytes []byte
		var err error

		// 缓存请求体内容
		if body != nil {
			bodyBytes, err = io.ReadAll(body)
			if err != nil {
				return nil, fmt.Errorf("failed to read request body: %v", err)
			}
		}

		for i := 0; i < maxRetries; i++ {
			var reqBody io.Reader
			if len(bodyBytes) > 0 {
				reqBody = bytes.NewReader(bodyBytes) // 每次重试使用缓存的请求体
			}

			var resp *http.Response
			if method == http.MethodGet {
				resp, err = http.Get(url)
			} else {
				resp, err = http.Post(url, "application/json", reqBody)
			}
			if err != nil {
				return nil, err
			}

			// 成功则直接返回
			if resp.StatusCode == http.StatusOK {
				return resp, nil
			}

			// 处理重定向逻辑
			// 现在其实可以稍微总结一下这个过程了，writer其实就是往body里面写入字节流罢了，字节流可以在这里用io.readAll全部读出来
			if resp.StatusCode == http.StatusForbidden {
				redirectBody, _ := io.ReadAll(resp.Body)
				resp.Body.Close()

				var leaderID uint64
				var leaderAddr string
				pairs := strings.Split(string(redirectBody), ",")
				for _, pair := range pairs {
					kv := strings.Split(pair, "=")
					if len(kv) != 2 {
						continue
					}
					switch kv[0] {
					case "LeaderID":
						leaderID, _ = strconv.ParseUint(kv[1], 10, 64)
					case "LeaderAddr":
						leaderAddr = kv[1]
					}
				}

				if leaderAddr == "" {
					return nil, fmt.Errorf("leader address is empty")
				}

				// 构造新 URL
				url = fmt.Sprintf(
					"http://%s/raft/%d%s",
					leaderAddr,
					leaderID,
					strings.TrimPrefix(url, fmt.Sprintf("http://%s/raft/%d", httpAddr, id)),
				)
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
		log.Println("send to : " + httpAddr)

		kv := map[string]string{"test_key": "test_value"}
		jsonData, _ := json.Marshal(kv)
		if len(jsonData) == 0 {
			t.Fatal("Empty JSON data")
		}

		url := fmt.Sprintf("http://%s/raft/%d/TxnSet", httpAddr, id)
		resp, err := sendRequestToLeader(http.MethodPost, url, bytes.NewBuffer(jsonData))
		if err != nil {
			t.Fatalf("PUT request failed after retries: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Unexpected status code: %d", resp.StatusCode)
		} else {
			t.Log("PUT request succeeded (leader)")
		}
	})
	t.Run("TestTxnPut", func(t *testing.T) {
		kv := map[string]string{"test_key2": "test_value2"}
		jsonData, _ := json.Marshal(kv)
		if len(jsonData) == 0 {
			t.Fatal("Empty JSON data")
		}

		url := fmt.Sprintf("http://%s/raft/%d/TxnSet", httpAddr, id)
		resp, err := sendRequestToLeader(http.MethodPost, url, bytes.NewBuffer(jsonData))
		if err != nil {
			t.Fatalf("PUT request failed after retries: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Unexpected status code: %d", resp.StatusCode)
		} else {
			t.Log("PUT request succeeded (leader)")
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
			t.Log("GET request succeeded (leader)")
			//var value string
			//if err := json.NewDecoder(resp.Body).Decode(&value); err != nil {
			//	t.Errorf("Failed to decode GET response: %v", err)
			//}
			//t.Logf("GET value: %s", value)
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

	t.Run("TestTxn", func(t *testing.T) {
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

func TestTxnRaftGet(t *testing.T) {
	key := "test_key2"

	configs := loadTestConfigs(t) // 提取配置加载逻辑

	for _, config := range configs {
		t.Run(fmt.Sprintf("Node%d", config.ID), func(t *testing.T) {
			getEndpoint := fmt.Sprintf("http://%s/raft/%d/get?key=%s",
				config.HttpServerAddr, config.ID, key)

			resp, err := http.Get(getEndpoint)
			if err != nil {
				t.Fatalf("HTTP GET failed: %v", err)
			}
			defer resp.Body.Close()

			// 验证状态码
			if resp.StatusCode != http.StatusOK &&
				resp.StatusCode != http.StatusNotFound {
				t.Fatalf("Unexpected status code: %d", resp.StatusCode)
			}

			// 解析响应
			var response struct {
				Value string `json:"value"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
				t.Fatalf("JSON decode failed: %v", err)
			}
			log.Printf(response.Value)
			// 验证结果
		})
	}
}

// 辅助函数：根据 leaderID 获取 Leader 的 HTTP 地址
func getLeaderAddr(leaderID uint64) string {
	// 这里需要实现从配置或服务发现中查找 Leader 的地址
	// 示例：硬编码或从全局配置中读取
	return "127.0.0.1:8080" // 替换为实际逻辑
}

func loadTestConfigs(t *testing.T) []*RaftConfig {
	// 提取公共配置加载逻辑
	var configs []*RaftConfig
	for _, path := range []string{
		"./configs/raft_config_1.yaml",
		"./configs/raft_config_2.yaml",
		"./configs/raft_config_3.yaml",
	} {
		config, err := LoadConfig(path)
		if err != nil {
			t.Fatalf("Load config failed: %v", err)
		}
		configs = append(configs, config)
	}
	return configs
}
