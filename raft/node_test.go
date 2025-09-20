package raft

import (
	"NucleusDB"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"testing"
)

// TEST NODE FUNCTIONS

// 测试是否会时间到期触发选举
// 计时器的设置是每一个节点都有一个对应的时钟触发器
func TestRaftNodeStart(t *testing.T) {
	// raft 配置
	config1, err := LoadConfig("./configs/raft_config_1.yaml")
	if err != nil {
		panic(err)
	}
	config2, err := LoadConfig("./configs/raft_config_2.yaml")
	if err != nil {
		panic(err)
	}
	config3, err := LoadConfig("./configs/raft_config_3.yaml")
	if err != nil {
		panic(err)
	}
	// 拿到数据库配置-注意一下哈，在每次启动的时候创建的数据目录全部都不一样，别到时候自己发现持久化有问题哈
	options1 := NucleusDB.DefaultOptions
	options2 := NucleusDB.DefaultOptions
	options3 := NucleusDB.DefaultOptions
	dir1, _ := os.MkdirTemp("", "raft1")
	dir2, _ := os.MkdirTemp("", "raft2")
	dir3, _ := os.MkdirTemp("", "raft3")

	options1.DirPath = dir1
	options2.DirPath = dir2
	options3.DirPath = dir3
	// 开三个节点慢慢开始联测-开启数据库调试
	StartNode(config1, options1)

	StartNode(config2, options2)

	StartNode(config3, options3)
	// 用一个无限循环 阻塞测试进程，
	// 否则在子线程调度的时候会将主进程退出从而无法进行测试
	for {

	}
}
func TestRaftPut(t *testing.T) {
	// 模拟客户端请求
	key := "testKey"
	value := "testValue"
	config1, err := LoadConfig("./configs/raft_config_1.yaml")
	if err != nil {
		panic(err)
	}
	config2, err := LoadConfig("./configs/raft_config_2.yaml")
	if err != nil {
		panic(err)
	}
	config3, err := LoadConfig("./configs/raft_config_3.yaml")
	if err != nil {
		panic(err)
	}
	// 将所有配置存储在一个切片中，以便查找
	configs := []*RaftConfig{config1, config2, config3}
	// 遍历每个节点，模拟客户端发送请求
	httpAddr := config1.HttpServerAddr
	id := config1.ID

	// 构造初始请求 URL
	putEndpoint := fmt.Sprintf("http://%s/raft/%d/put", httpAddr, id)

	// 构造 JSON 请求体
	kv := map[string]string{
		key: value,
	}
	jsonBody, err := json.Marshal(kv)
	if err != nil {
		t.Errorf("Failed to marshal JSON body: %v", err)
		return
	}
	var resp *http.Response
	// 尝试发送请求，直到成功或者达到最大重试次数
	maxRetries := 3
	for retry := 0; retry < maxRetries; retry++ {
		// 使用 bytes.Buffer 包装请求体
		bodyReader := bytes.NewReader(jsonBody)
		resp, err = http.Post(putEndpoint, "application/json", bodyReader)
		if err != nil {
			t.Errorf("Failed to send request to node %d: %v (retry %d)", id, err, retry)
			continue
		}

		// 如果响应状态码不是禁止，说明请求成功
		if resp.StatusCode != http.StatusForbidden {
			break
		}

		// 读取响应体，获取 Leader ID
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Errorf("Failed to read response body from node %d: %v", id, err)
			continue
		}
		resp.Body.Close()

		// 将响应体转换为 Leader ID
		leaderID, err := strconv.Atoi(string(body))
		if err != nil {
			t.Errorf("Failed to parse leader ID from node %d: %v", id, err)
			break
		}

		// 查找 Leader 的 HTTP 地址
		var leaderHttpAddr string
		for _, c := range configs {
			if c.ID == uint64(leaderID) {
				leaderHttpAddr = c.HttpServerAddr
				break
			}
		}
		if leaderHttpAddr == "" {
			t.Errorf("Leader ID %d not found in configs", leaderID)
			break
		}

		// 构造新的请求 URL
		putEndpoint = fmt.Sprintf("http://%s/raft/%d/put", leaderHttpAddr, leaderID)

		// 重置请求体读取器
		bodyReader = bytes.NewReader(jsonBody)
	}

	if err != nil {
		t.Errorf("Failed to send request to node %d after %d retries: %v", id, maxRetries, err)
		return
	}

	// 检查最终响应
	if resp != nil {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Errorf("Failed to read final response body from node %d: %v", id, err)
			return
		}
		log.Printf("Response from node %d: %s", id, body)
	}

}

func TestRaftGet(t *testing.T) {
	// 模拟客户端请求
	key := "key2"
	config1, err := LoadConfig("./configs/raft_config_1.yaml")
	if err != nil {
		panic(err)
	}
	config2, err := LoadConfig("./configs/raft_config_2.yaml")
	if err != nil {
		panic(err)
	}
	config3, err := LoadConfig("./configs/raft_config_3.yaml")
	if err != nil {
		panic(err)
	}
	configs := []*RaftConfig{config1, config2, config3}
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

// 测试记忆设置功能
func TestRaftMemSet(t *testing.T) {
	key := "0"
	value := "testValue"
	config1, err := LoadConfig("./configs/raft_config_1.yaml")
	if err != nil {
		panic(err)
	}
	config2, err := LoadConfig("./configs/raft_config_2.yaml")
	if err != nil {
		panic(err)
	}
	config3, err := LoadConfig("./configs/raft_config_3.yaml")
	if err != nil {
		panic(err)
	}
	// 将所有配置存储在一个切片中，以便查找
	configs := []*RaftConfig{config1, config2, config3}
	// 遍历每个节点，模拟客户端发送请求
	httpAddr := config1.HttpServerAddr
	id := config1.ID

	// 构造初始请求 URL
	putEndpoint := fmt.Sprintf("http://%s/raft/%d/MemSet", httpAddr, id)
	type data struct {
		AgentId string `json:"agentId"`
		Value   string `json:"value"`
	}
	// 构造 JSON 请求体
	dataBody := data{
		AgentId: key,
		Value:   value,
	}
	jsonData, err := json.Marshal(dataBody)
	if err != nil {
		log.Fatalf("JSON marshal error: %v", err)
	}
	var resp *http.Response
	// 尝试发送请求，直到成功或者达到最大重试次数
	maxRetries := 3
	for retry := 0; retry < maxRetries; retry++ {
		// 使用 bytes.Buffer 包装请求体
		bodyReader := bytes.NewReader(jsonData)
		resp, err = http.Post(putEndpoint, "application/json", bodyReader)
		if err != nil {
			t.Errorf("Failed to send request to node %d: %v (retry %d)", id, err, retry)
			continue
		}

		// 如果响应状态码不是禁止，说明请求成功
		if resp.StatusCode != http.StatusForbidden {
			break
		}

		// 读取响应体，获取 Leader ID
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Errorf("Failed to read response body from node %d: %v", id, err)
			continue
		}
		resp.Body.Close()

		// 将响应体转换为 Leader ID
		leaderID, err := strconv.Atoi(string(body))
		if err != nil {
			t.Errorf("Failed to parse leader ID from node %d: %v", id, err)
			break
		}

		// 查找 Leader 的 HTTP 地址
		var leaderHttpAddr string
		for _, c := range configs {
			if c.ID == uint64(leaderID) {
				leaderHttpAddr = c.HttpServerAddr
				break
			}
		}
		if leaderHttpAddr == "" {
			t.Errorf("Leader ID %d not found in configs", leaderID)
			break
		}

		// 构造新的请求 URL
		putEndpoint = fmt.Sprintf("http://%s/raft/%d/MemSet", leaderHttpAddr, leaderID)

		// 重置请求体读取器
		bodyReader = bytes.NewReader(jsonData)
	}

	if err != nil {
		t.Errorf("Failed to send request to node %d after %d retries: %v", id, maxRetries, err)
		return
	}

	// 检查最终响应
	if resp != nil {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Errorf("Failed to read final response body from node %d: %v", id, err)
			return
		}
		log.Printf("Response from node %d: %s", id, body)
	}

}

func TestRaftMemGet(t *testing.T) {
	type MemGetResponse struct {
		AgentId string `json:"agentId"`
		Value   string `json:"value"`
	}
	key := "0"
	config1, _ := LoadConfig("./configs/raft_config_1.yaml")
	config2, _ := LoadConfig("./configs/raft_config_1.yaml")
	config3, _ := LoadConfig("./configs/raft_config_1.yaml")

	configs := []*RaftConfig{
		config1, config2, config3,
	}

	for _, config := range configs {
		httpAddr := config.HttpServerAddr
		id := config.ID

		getEndpoint := fmt.Sprintf("http://%s/raft/%d/MemGet?agentId=%s", httpAddr, id, key)

		// 发送 GET 请求
		resp, err := http.Get(getEndpoint)
		if err != nil {
			t.Fatalf("Failed to send GET request to node %d: %v", id, err)
			continue
		}
		defer resp.Body.Close()

		// 检查状态码
		if resp.StatusCode != http.StatusOK {
			t.Errorf("Unexpected status code: %d", resp.StatusCode)
			continue
		}

		// 解析响应体
		var result string
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			t.Fatalf("Failed to parse JSON response: %v", err)
			continue
		}
		log.Printf("Response from node %d: %s", id, result)
	}
}
