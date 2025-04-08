package txn

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// CoordinatorClient 协调者客户端，用于与协调者HTTP服务交互
type CoordinatorClient struct {
	baseURL    string
	httpClient *http.Client
}

// NewCoordinatorClient 创建新的协调者客户端实例
func NewCoordinatorClient(serverAddr string) *CoordinatorClient {
	return &CoordinatorClient{
		baseURL:    "http://" + serverAddr,
		httpClient: &http.Client{Timeout: 50 * time.Second},
	}
}

// HandleConflictCheck 客户端冲突检测方法
func (c *CoordinatorClient) HandleConflictCheck(checkKeys []uint64, startTime, commitTime uint64) (bool, error) {
	req := conflictCheckRequest{
		CheckKeys:  checkKeys,
		StartTime:  startTime,
		CommitTime: commitTime,
	}

	body, err := json.Marshal(req)
	if err != nil {
		return false, fmt.Errorf("marshal request failed: %v", err)
	}
	resp, err := c.httpClient.Post(c.baseURL+"/conflict-check", "application/json", bytes.NewReader(body))
	if err != nil {
		return false, fmt.Errorf("HTTP request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var result conflictCheckResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return false, fmt.Errorf("decode response failed: %v", err)
	}

	if result.Error != "" {
		return false, fmt.Errorf(result.Error)
	}
	return result.HasConflict, nil
}

// SaveSnapshot 客户端保存快照方法
func (c *CoordinatorClient) SaveSnapshot(snapshot *TxnSnapshot) error {
	req := saveSnapshotRequest{Snapshot: snapshot}
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request failed: %v", err)
	}
	resp, err := c.httpClient.Post(c.baseURL+"/save-snapshot", "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("HTTP request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	return nil
}
