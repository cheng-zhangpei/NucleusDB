package txn_client

import (
	"ComDB/raft"
	"bytes"
	"encoding/json"
	"fmt"
	_ "github.com/stretchr/testify/assert/yaml"
	"io"
	_ "io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
)

// 封装事务操作

type txnClient struct {
	raftConfig *raft.RaftConfig
}

func NewTxnClient(filePath string) *txnClient {
	config, err := raft.LoadConfig(filePath)
	if err != nil {
		panic(err)
	}
	return &txnClient{raftConfig: config}
}

func (tc *txnClient) Put(key, value string) error {
	httpAddr := tc.raftConfig.HttpServerAddr
	id := tc.raftConfig.ID
	log.Println("send to : " + httpAddr)

	kv := map[string]string{key: value}
	jsonData, _ := json.Marshal(kv)
	if len(jsonData) == 0 {
		log.Fatal("Empty JSON data")
	}
	log.Println(httpAddr)
	url := fmt.Sprintf("http://%s/raft/%d/TxnSet", httpAddr, id)
	resp, err := tc.sendRequestWithRedirect(http.MethodPost, url, bytes.NewBuffer(jsonData))
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

func (tc *txnClient) Delete(key string) error {
	httpAddr := tc.raftConfig.HttpServerAddr
	id := tc.raftConfig.ID
	url := fmt.Sprintf("http://%s/raft/%d/TxnDelete?key=%s", httpAddr, id, key)

	resp, err := tc.sendRequestWithRedirect(http.MethodPost, url, nil)
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

func (tc *txnClient) Get(key string) error {
	httpAddr := tc.raftConfig.HttpServerAddr
	id := tc.raftConfig.ID
	url := fmt.Sprintf("http://%s/raft/%d/TxnGet?key=%s", httpAddr, id, key)

	resp, err := tc.sendRequestWithRedirect(http.MethodGet, url, nil)
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

func (tc *txnClient) Commit() error {
	httpAddr := tc.raftConfig.HttpServerAddr
	id := tc.raftConfig.ID

	url := fmt.Sprintf("http://%s/raft/%d/TxnCommit", httpAddr, id)

	resp, err := tc.sendRequestWithRedirect(http.MethodPost, url, nil)
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
func (tc *txnClient) GetResult() ([]string, error) {
	httpAddr := tc.raftConfig.HttpServerAddr
	id := tc.raftConfig.ID
	url := fmt.Sprintf("http://%s/raft/%d/TxnGetResult",
		httpAddr, id)
	resp, err := tc.sendRequestWithRedirect(http.MethodPost, url, nil)
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

func (tc *txnClient) setStartTime() error {
	httpAddr := tc.raftConfig.HttpServerAddr
	id := tc.raftConfig.ID
	url := fmt.Sprintf("http://%s/raft/%d/startTs",
		httpAddr, id)
	resp, err := tc.sendRequestWithRedirect(http.MethodPost, url, nil)
	if err != nil {
		return fmt.Errorf("HTTP GET failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	return nil
}

// 封装一下让用户可以直接通过update来操作集群，而不是分散的进行事务操作
// 返回其中get的内容
func (tc *txnClient) update(fn func(tc *txnClient) error) ([]string, error) {
	// todo 我暂时把事务的startTs放到这个位置生成，这种方法其实不太好，但是现在需要先让流程跑起来
	// todo 暂时是因为客户端和服务端都在本机，否则这是完全不合理的
	err := tc.setStartTime()
	if err != nil {
		return nil, err
	}
	if err := fn(tc); err != nil {
		return nil, err
	}
	// 我们在update内部调用commit
	err = tc.Commit()
	if err != nil {
		return nil, err
	}
	result, err := tc.GetResult()
	if err != nil {
		return nil, err
	}
	return result, nil
}

// ============================  重定向辅助函数 =====================================

func (tc *txnClient) sendRequestWithRedirect(method, url string, body io.Reader) (*http.Response, error) {
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
	httpAddr := tc.raftConfig.HttpServerAddr
	id := tc.raftConfig.ID
	for i := 0; i < maxRetries; i++ {
		var reqBody io.Reader
		if len(bodyBytes) > 0 {
			reqBody = bytes.NewReader(bodyBytes) // 每次重试使用缓存的请求体
		}
		var leaderID uint64
		var leaderAddr string
		var resp *http.Response
		log.Printf("重定向url: %s", url)
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
