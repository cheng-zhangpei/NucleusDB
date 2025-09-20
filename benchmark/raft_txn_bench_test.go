package benchmark

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"NucleusDB/utils"
)

// 事务操作类型
const (
	ShortTxnPrefix = "bench_short"
	LongTxnPrefix  = "bench_long"
)

// Benchmark_RaftShortTxn 测试短事务（单次操作）
func Benchmark_RaftShortTxn(b *testing.B) {
	initCluster()
	endpoint := getLeaderEndpoint("txn")

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		client := &http.Client{
			Timeout: 5 * time.Second,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 100,
			},
		}

		counter := 0
		for pb.Next() {
			// 构造短事务（单次PUT）
			tx := TransactionRequest{
				Operations: []TxOperation{
					{
						Type:  "PUT",
						Key:   fmt.Sprintf("%s_%d_%d", ShortTxnPrefix, time.Now().UnixNano(), counter),
						Value: utils.RandomValue(100),
					},
				},
			}
			counter++

			// 序列化请求
			body, _ := json.Marshal(tx)

			// 发送并重试
			if err := sendTxnWithRetry(client, endpoint, body); err != nil {
				b.Fatalf("Txn failed: %v", err)
			}
		}
	})
}

// Benchmark_RaftLongTxn 测试长事务（批量操作）
func Benchmark_RaftLongTxn(b *testing.B) {
	initCluster()
	endpoint := getLeaderEndpoint("txn")

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		client := &http.Client{
			Timeout: 10 * time.Second,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 100,
			},
		}

		batchCounter := 0
		for pb.Next() {
			// 构造长事务（1000次PUT）
			var ops []TxOperation
			baseKey := fmt.Sprintf("%s_%d_%d", LongTxnPrefix, time.Now().UnixNano(), batchCounter)

			for i := 0; i < 1000; i++ {
				ops = append(ops, TxOperation{
					Type:  "PUT",
					Key:   fmt.Sprintf("%s_%d", baseKey, i),
					Value: utils.RandomValue(1024),
				})
			}
			batchCounter++

			// 序列化请求
			body, _ := json.Marshal(TransactionRequest{Operations: ops})

			// 发送并重试
			if err := sendTxnWithRetry(client, endpoint, body); err != nil {
				b.Fatalf("Txn failed: %v", err)
			}
		}
	})
}

// 事务请求结构
type TransactionRequest struct {
	Operations []TxOperation `json:"ops"`
}

type TxOperation struct {
	Type  string `json:"type"`
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

// 带重试的事务提交
func sendTxnWithRetry(client *http.Client, endpoint string, body []byte) error {
	maxRetries := 3
	currentEndpoint := endpoint

	for i := 0; i < maxRetries; i++ {
		resp, err := client.Post(currentEndpoint, "application/json", bytes.NewReader(body))
		if err != nil {
			continue
		}
		defer resp.Body.Close()

		switch resp.StatusCode {
		case http.StatusOK:
			return nil
		case http.StatusForbidden:
			// 更新 Leader 端点
			newLeader := parseLeaderFromHeader(resp.Header)
			if newLeader != "" {
				currentEndpoint = newLeader
				continue
			}
		}
	}
	return fmt.Errorf("max retries exceeded")
}

// 从响应头解析 Leader 信息
func parseLeaderFromHeader(header http.Header) string {
	if leaderID := header.Get("X-Leader-ID"); leaderID != "" {
		for _, cfg := range configs {
			if fmt.Sprintf("%d", cfg.ID) == leaderID {
				return fmt.Sprintf("http://%s/raft/%d/txn", cfg.HttpServerAddr, cfg.ID)
			}
		}
	}
	return ""
}
