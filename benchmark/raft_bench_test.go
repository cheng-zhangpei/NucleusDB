package benchmark

import (
	"ComDB/raft"
	"bytes"
	"encoding/json"
	"fmt"
	"golang.org/x/exp/rand"
	"io"
	"net/http"
	"strconv"
	"testing"
	"time"

	"ComDB/utils"
	"github.com/stretchr/testify/assert"
)

// 集群配置信息（根据实际配置修改）
const (
	raftConfigPath1 = "../raft/configs/raft_config_1.yaml"
	raftConfigPath2 = "../raft/configs/raft_config_2.yaml"
	raftConfigPath3 = "../raft/configs/raft_config_3.yaml"
)

// 全局测试配置缓存
var (
	configs     []*raft.RaftConfig
	leaderAddr  string
	initialized bool
)

func initCluster() {
	if initialized {
		return
	}

	// 加载所有节点配置
	loadConfig := func(path string) *raft.RaftConfig {
		cfg, err := raft.LoadConfig(path)
		if err != nil {
			panic(fmt.Sprintf("Load config failed: %v", err))
		}
		return cfg
	}

	configs = []*raft.RaftConfig{
		loadConfig(raftConfigPath1),
		loadConfig(raftConfigPath2),
		loadConfig(raftConfigPath3),
	}

	// 等待集群选举完成（简单实现）
	time.Sleep(3 * time.Second)
	initialized = true
}

var (
	leaderID       uint64 // 缓存 Leader 的 ID
	leaderAddrTemp string // 缓存 Leader 的地址
	configsTemp    []*raft.RaftConfig
)

func getLeaderEndpoint(operation string) string {
	if leaderAddr != "" && leaderID != 0 {
		return fmt.Sprintf("http://%s/raft/%d/%s", leaderAddr, leaderID, operation)
	}

	// 探测所有节点查找 Leader
	for _, cfg := range configs {
		url := fmt.Sprintf("http://%s/raft/%d/put", cfg.HttpServerAddr, cfg.ID)
		resp, err := http.Post(url, "application/json", bytes.NewReader([]byte("{}")))
		if err != nil {
			continue
		}
		defer resp.Body.Close()

		// Case 1: 当前节点是 Leader
		if resp.StatusCode != http.StatusForbidden {
			leaderID = cfg.ID
			leaderAddr = cfg.HttpServerAddr
			return fmt.Sprintf("http://%s/raft/%d/%s", leaderAddr, leaderID, operation)
		}

		// Case 2: 从响应中获取 Leader ID
		body, _ := io.ReadAll(resp.Body)
		if id, err := strconv.ParseUint(string(body), 10, 64); err == nil {
			// 根据 ID 查找对应的节点配置
			for _, c := range configs {
				if c.ID == id {
					leaderID = id
					leaderAddr = c.HttpServerAddr
					return fmt.Sprintf("http://%s/raft/%d/%s", leaderAddr, leaderID, operation)
				}
			}
		}
	}
	panic("Cannot find leader node")
}

// Benchmark_RaftPut 测试 PUT 操作吞吐量（包含独立数据准备）
func Benchmark_RaftPut(b *testing.B) {
	initCluster()
	endpoint := getLeaderEndpoint("put")

	// 生成唯一测试键值前缀
	testPrefix := fmt.Sprintf("bench_put_%d_", time.Now().UnixNano())

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		client := &http.Client{
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 100, // 连接池优化
			},
			Timeout: 5 * time.Second,
		}

		var counter int
		for pb.Next() {
			// 生成唯一键避免冲突
			key := utils.GetTestKey(counter)
			counter++

			// 构造带前缀的测试键
			fullKey := fmt.Sprintf("%s%d", testPrefix, key)
			kv := map[string]string{fullKey: "value"}

			// 序列化请求
			body := &bytes.Buffer{}
			json.NewEncoder(body).Encode(kv)

			// 发送请求
			resp, err := client.Post(endpoint, "application/json", body)
			if !assert.NoError(b, err) {
				continue
			}

			// 验证响应状态
			if resp.StatusCode != http.StatusOK {
				b.Logf("Unexpected status code: %d", resp.StatusCode)
			}

			// 清理响应资源
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	})
}

// Benchmark_RaftGet 测试 GET 操作吞吐量（含完整数据准备）
func Benchmark_RaftGet(b *testing.B) {
	initCluster()
	endpoint := getLeaderEndpoint("get")

	// 准备阶段：插入测试数据
	const preloadCount = 10000
	preloadKeys := prepareTestData("bench_get", preloadCount)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		client := &http.Client{
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 100,
			},
			Timeout: 5 * time.Second,
		}

		rand.Seed(uint64(time.Now().UnixNano()))
		for pb.Next() {
			// 随机选择预加载的key
			key := preloadKeys[rand.Intn(len(preloadKeys))]
			url := fmt.Sprintf("%s?key=%s", endpoint, key)

			resp, err := client.Get(url)
			if !assert.NoError(b, err) {
				continue
			}

			// 验证数据有效性
			if resp.StatusCode == http.StatusOK {
				var value string
				if err := json.NewDecoder(resp.Body).Decode(&value); err != nil {
					b.Logf("Decode error: %v", err)
				}
			}

			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	})
}

func Benchmark_RaftDelete(b *testing.B) {
	initCluster()
	endpoint := getLeaderEndpoint("delete")

	// 准备阶段：插入待删除数据
	testPrefix := fmt.Sprintf("bench_del_%d_", time.Now().UnixNano())
	keys := prepareTestData(testPrefix, b.N)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		client := &http.Client{
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 100,
			},
			Timeout: 5 * time.Second,
		}

		var idx int
		for pb.Next() {
			if idx >= len(keys) {
				idx = 0
			}
			key := keys[idx]
			idx++

			// 修正点1: 使用 POST 方法并携带参数
			req, _ := http.NewRequest("POST", endpoint, nil)
			q := req.URL.Query()
			q.Add("key", key)
			req.URL.RawQuery = q.Encode()

			// 发送请求
			resp, err := client.Do(req)
			if !assert.NoError(b, err) {
				continue
			}

			// 验证删除结果
			assert.Equal(b, http.StatusOK, resp.StatusCode)

			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	})
}

// 辅助函数：发送请求并处理重定向
func sendRequest(op string, data map[string]string) {
	endpoint := getLeaderEndpoint(op)
	body, _ := json.Marshal(data)
	resp, _ := http.Post(endpoint, "application/json", bytes.NewReader(body))
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}

// 数据准备工具函数
func prepareTestData(prefix string, count int) []string {
	endpoint := getLeaderEndpoint("put")
	client := &http.Client{Timeout: 10 * time.Second}

	keys := make([]string, count)
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("%s%d", prefix, i)
		keys[i] = key

		kv := map[string]string{key: "bench_data"}
		body := &bytes.Buffer{}
		json.NewEncoder(body).Encode(kv)

		resp, err := client.Post(endpoint, "application/json", body)
		if err != nil || resp.StatusCode != http.StatusOK {
			panic(fmt.Sprintf("Preload data failed: %v", err))
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	return keys
}
