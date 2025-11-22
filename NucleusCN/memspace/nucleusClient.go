package memspace

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// NucleusClient us dbClient to connect with nucleusDB
type NucleusClient struct {
	HttpServer string
	raftId     int
}

func NewNucleusClient(HttpServer string, raftId int) *NucleusClient {
	return &NucleusClient{HttpServer: HttpServer, raftId: raftId}
}

// TxnGet :get value from the distributed database
func (nc *NucleusClient) TxnGet(key []byte) error {
	httpAddr := nc.HttpServer
	id := nc.raftId
	url := fmt.Sprintf("http://%s/raft/%d/TxnGet?key=%s", httpAddr, id, key)

	resp, err := nc.sendRequestWithRedirect(http.MethodGet, url, nil)
	if err != nil {
		log.Fatalf("GET request failed after retries: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		log.Printf("GET request succeeded (leader)")
	} else {
		log.Printf("Unexpected status code: %d", resp.StatusCode)
	}
	return err
}

// TxnPut :set value into distributed database
func (nc *NucleusClient) TxnPut(key []byte, value []byte) error {
	httpAddr := nc.HttpServer
	id := nc.raftId

	kv := map[string]string{string(key): string(value)}
	jsonData, _ := json.Marshal(kv)
	if len(jsonData) == 0 {
		log.Fatal("Empty JSON data")
	}

	url := fmt.Sprintf("http://%s/raft/%d/TxnSet", httpAddr, id)
	resp, err := nc.sendRequestWithRedirect(http.MethodPost, url, bytes.NewBuffer(jsonData))
	if err != nil {
		log.Fatalf("PUT request failed after retries: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Fatalf("Unexpected status code: %d", resp.StatusCode)
	} else {
		log.Printf("PUT request succeeded (leader)")
	}
	return err
}

// TxnDelete :Delete data in a distributed database
func (nc *NucleusClient) TxnDelete(key []byte) error {
	httpAddr := nc.HttpServer
	id := nc.raftId
	url := fmt.Sprintf("http://%s/raft/%d/TxnDelete?key=%s", httpAddr, id, key)

	resp, err := nc.sendRequestWithRedirect(http.MethodPost, url, nil)
	if err != nil {
		log.Fatalf("DELETE request failed after retries: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		log.Printf("DELETE request succeeded (leader)")
	} else {
		log.Printf("Unexpected status code: %d", resp.StatusCode)
	}
	return err
}

// Commit commit Txn
func (nc *NucleusClient) Commit() error {
	httpAddr := nc.HttpServer
	id := nc.raftId

	url := fmt.Sprintf("http://%s/raft/%d/TxnCommit", httpAddr, id)

	resp, err := nc.sendRequestWithRedirect(http.MethodPost, url, nil)
	if err != nil {
		log.Fatalf("Commit request failed after retries: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		log.Println("Commit succeeded")
	} else {
		log.Fatalf("Unexpected status code: %d", resp.StatusCode)
	}
	return err
}

// Update submit the transaction
func (nc *NucleusClient) Update(fn func(TxnOperation) error) ([]string, error) {
	err := nc.setStartTime()
	if err != nil {
		return nil, err
	}
	if err := fn(nc); err != nil {
		return nil, err
	}

	err = nc.Commit()
	if err != nil {
		return nil, err
	}
	// todo 思考如何比较好的将事物数据返回给用户呢？这是需要思考滴
	result, err := nc.GetResult()
	if err != nil {
		return nil, err
	}
	return result, nil
}

// DistributeGet I can get value from any node of the cluster
func (nc *NucleusClient) DistributeGet(key []byte) ([]byte, error) {
	httpAddr := nc.HttpServer
	id := nc.raftId
	// 构造初始请求 URL
	getEndpoint := fmt.Sprintf("http://%s/raft/%d/get?key=%s", httpAddr, id, string(key))
	// 发送 GET 请求
	resp, err := http.Get(getEndpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to send GET request: %v", err)
	}
	defer resp.Body.Close()
	// 检查HTTP状态码
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP request failed with status: %s", resp.Status)
	}
	// 读取响应体
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %v", err)
	}
	// 解析JSON响应
	var valueStr string
	if err := json.Unmarshal(body, &valueStr); err != nil {
		return nil, fmt.Errorf("failed to parse JSON response: %v", err)
	}
	if len(valueStr) == 0 {
		return []byte(""), ErrKeyNotFound
	}
	log.Printf("Successfully retrieved value via HTTP: key=%s, value=%s", string(key), valueStr)
	return []byte(valueStr), nil
}

// DistributePut put operation in cluster mod
func (nc *NucleusClient) DistributePut(key []byte, value []byte) error {
	httpAddr := nc.HttpServer
	id := nc.raftId
	putEndpoint := fmt.Sprintf("http://%s/raft/%d/put", httpAddr, id)
	kv := map[string]string{string(key): string(value)}
	jsonData, _ := json.Marshal(kv)
	if len(jsonData) == 0 {
		log.Fatal("Empty JSON data")
	}
	// direct msg to leader
	resp, err := nc.sendRequestWithRedirect(http.MethodPost, putEndpoint, bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	// 检查最终响应
	if resp != nil {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		log.Printf("Response from node %d: %s", id, body)
	}
	return nil
}

// DistributeDelete delete operation in cluster mod
func (nc *NucleusClient) DistributeDelete(key []byte) error {
	httpAddr := nc.HttpServer
	id := nc.raftId
	deleteEndpoint := fmt.Sprintf("http://%s/raft/%d/delete", httpAddr, id)
	// 创建请求体（如果需要的话，根据服务端实现调整）
	// 由于服务端通过 URL query 参数获取 key，这里可以传空 body 或者包含 key 的 JSON
	kv := map[string]string{"key": string(key)}
	jsonData, _ := json.Marshal(kv)
	if len(jsonData) == 0 {
		return fmt.Errorf("empty JSON data")
	}

	// 发送请求，支持重定向到 leader
	resp, err := nc.sendRequestWithRedirect(http.MethodPost, deleteEndpoint, bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	defer func() {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
	}()

	// 检查最终响应
	if resp != nil {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		log.Printf("Response from node %d: %s", id, string(body))

		// 检查是否成功
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("delete failed with status %d: %s", resp.StatusCode, string(body))
		}
	}
	return nil
}
func (nc *NucleusClient) DistributePrefixList(key []byte) ([][]byte, error) {
	httpAddr := nc.HttpServer
	id := nc.raftId
	url := fmt.Sprintf("http://%s/raft/%d/prefix?key=%s", httpAddr, id, url.QueryEscape(string(key)))

	resp, err := nc.sendRequestWithRedirect(http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("prefix list request failed after retries: %v", err)
	}
	defer func() {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
	}()

	// 检查HTTP状态码
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusNotFound {
			return [][]byte{}, nil // 返回空数组而不是错误
		}
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("prefix list failed with status %d: %s", resp.StatusCode, string(body))
	}

	// 读取并解析JSON响应
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %v", err)
	}

	// 解析JSON数组
	var valueStrings []string
	if err := json.Unmarshal(body, &valueStrings); err != nil {
		return nil, fmt.Errorf("failed to parse JSON response: %v", err)
	}

	// 转换为 [][]byte
	var results [][]byte
	for _, str := range valueStrings {
		results = append(results, []byte(str))
	}

	log.Printf("Successfully retrieved %d values for prefix: %s", len(results), string(key))
	return results, nil
}

// ======================================some functional methods============================================================
// setStartTime A helper function for setting timestamps; not all metadata stores require this function.
func (nc *NucleusClient) setStartTime() error {
	httpAddr := nc.HttpServer
	id := nc.raftId
	url := fmt.Sprintf("http://%s/raft/%d/startTs",
		httpAddr, id)
	resp, err := nc.sendRequestWithRedirect(http.MethodPost, url, nil)
	if err != nil {
		return fmt.Errorf("HTTP GET failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	return nil
}

// sendRequestWithRedirect Used to direct requests to the leader
func (nc *NucleusClient) sendRequestWithRedirect(method, url string, body io.Reader) (*http.Response, error) {
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
	httpAddr := nc.HttpServer
	id := nc.raftId
	for i := 0; i < maxRetries; i++ {
		var reqBody io.Reader
		if len(bodyBytes) > 0 {
			reqBody = bytes.NewReader(bodyBytes) // 每次重试使用缓存的请求体
		}
		var leaderID uint64
		var leaderAddr string
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
			log.Printf("Redirecting to leader %d at %s", leaderID, leaderAddr)
			continue
		}
		// 其他错误直接返回
		return resp, nil
	}
	return nil, fmt.Errorf("max retries exceeded")
}

func (nc *NucleusClient) GetResult() ([]string, error) {
	httpAddr := nc.HttpServer
	id := nc.raftId
	url := fmt.Sprintf("http://%s/raft/%d/TxnGetResult",
		httpAddr, id)
	resp, err := nc.sendRequestWithRedirect(http.MethodPost, url, nil)
	if err != nil {
		return nil, fmt.Errorf("HTTP GET failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %v", err)
	}

	var result []string
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %v", err)
	}
	return result, nil
}
