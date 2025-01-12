package main

import (
	ComDB "ComDB"
	"ComDB/search"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
)

var db *ComDB.DB

func init() {
	// 初始化 DB 实例
	var err error
	options := ComDB.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-http")
	options.DirPath = dir
	db, err = ComDB.Open(options)
	if err != nil {
		panic(fmt.Sprintf("failed to open db: %v", err))
	}
	log.Println("Database created successfully at:", dir)
}

func handlePut(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var kv map[string]string

	if err := json.NewDecoder(request.Body).Decode(&kv); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		log.Printf("Failed to decode request body: %v\n", err)
		return
	}

	for key, value := range kv {
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			log.Printf("Failed to put key-value pair in db: key=%s, value=%s, error=%v\n", key, value, err)
			return
		}
		log.Printf("Key-value pair inserted successfully: key=%s, value=%s\n", key, value)
	}
}

func handleGet(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := request.URL.Query().Get("key")
	log.Printf("Received GET request for key: %s\n", key)

	value, err := db.Get([]byte(key))
	if err != nil && err != ComDB.ErrKeyNotFound {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		log.Printf("Failed to get value for key: key=%s, error=%v\n", key, err)
		return
	}

	if err == ComDB.ErrKeyNotFound {
		log.Printf("Key not found: key=%s\n", key)
	} else {
		log.Printf("Successfully retrieved value for key: key=%s, value=%s\n", key, string(value))
	}

	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(string(value))
}

func handleDelete(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodDelete {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := request.URL.Query().Get("key")
	log.Printf("Received DELETE request for key: %s\n", key)

	err := db.Delete([]byte(key))
	if len(key) == 0 {
		log.Printf("Key is empty: key=%s\n", key)
	}
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		log.Printf("Failed to delete key: key=%s, error=%v\n", key, err)
		return
	}
	log.Printf("Key deleted successfully: key=%s\n", key)

	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode("OK")
}

func handleListKeys(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	log.Println("Received request to list all keys")

	keys := db.ListKeys()
	writer.Header().Set("Content-Type", "application/json")
	var result []string
	for _, k := range keys {
		result = append(result, string(k))
	}
	log.Printf("Listed all keys: %v\n", result)
	_ = json.NewEncoder(writer).Encode(result)
}

func handleStat(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	log.Println("Received request to get database statistics")

	stat := db.Stat()
	writer.Header().Set("Content-Type", "application/json")
	log.Printf("Database statistics: %+v\n", stat)
	_ = json.NewEncoder(writer).Encode(stat)
}

func handlePrefix(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 从查询参数中获取前缀
	prefix := request.URL.Query().Get("prefix")
	if prefix == "" {
		http.Error(writer, "prefix parameter is required", http.StatusBadRequest)
		log.Println("Prefix parameter is missing in request")
		return
	}

	log.Printf("Received request to query keys with prefix: %s\n", prefix)

	// 使用迭代器遍历匹配前缀的键值对
	iterator := db.NewIterator(ComDB.IteratorOptions{
		Prefix:  []byte(prefix),
		Reverse: false, // 是否反向遍历
	})
	defer iterator.Close()

	// 存储匹配的键值对
	results := make(map[string]string)
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		key := iterator.Key()
		value, err := db.Get(key)
		if err != nil {
			log.Printf("Failed to get value for key %s: %v\n", string(key), err)
			continue
		}
		results[string(key)] = string(value)
	}

	log.Printf("Found %d keys with prefix %s\n", len(results), prefix)

	// 返回 JSON 格式的结果
	writer.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(writer).Encode(results); err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		log.Printf("Failed to encode response: %v\n", err)
		return
	}
}

// ===================================== Memory ===================================
// handleMemoryGet 处理获取记忆的请求
func handleMemoryGet(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 从查询参数中获取 agentId
	agentId := request.URL.Query().Get("agentId")
	if agentId == "" {
		http.Error(writer, "agentId parameter is required", http.StatusBadRequest)
		return
	}

	// 创建 MemoryStructure 实例
	ms := &search.MemoryStructure{
		Db: db,
	}

	// 调用 MMGet 方法
	memory, err := ms.MMGet(agentId)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	// 返回结果
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(memory)
}

// handleMemorySet 处理设置记忆的请求
func handleMemorySet(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 解析请求体
	var data struct {
		AgentId string `json:"agentId"`
		Value   string `json:"value"`
	}
	if err := json.NewDecoder(request.Body).Decode(&data); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}

	// 创建 MemoryStructure 实例
	ms := &search.MemoryStructure{
		Db: db,
	}

	// 调用 MMSet 方法
	if err := ms.MMSet([]byte(data.Value), data.AgentId); err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	// 返回成功响应
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode("OK")
}

// handleMemorySearch 处理搜索记忆的请求
func handleMemorySearch(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 从查询参数中获取 agentId 和 searchItem
	agentId := request.URL.Query().Get("agentId")
	searchItem := request.URL.Query().Get("searchItem")
	if agentId == "" || searchItem == "" {
		http.Error(writer, "agentId and searchItem parameters are required", http.StatusBadRequest)
		return
	}

	// 创建 MemoryStructure 实例
	ms := &search.MemoryStructure{
		Db: db,
	}

	// 调用 matchSearch 方法
	result, err := ms.MatchSearch(searchItem, agentId)

	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	// 返回结果
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(result)
}

// handleCreateMemoryMeta 处理创建记忆空间的请求
func handleCreateMemoryMeta(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// 解析请求体
	var data struct {
		AgentId   string `json:"agentId"`
		TotalSize int64  `json:"totalSize"`
	}
	if err := json.NewDecoder(request.Body).Decode(&data); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	// 创建记忆空间 ->
	meta := search.NewMemoryMeta(data.AgentId, data.TotalSize)
	enMeta := meta.Encode()
	// 现在是一个空的记忆空间
	_ = db.Put([]byte(data.AgentId), enMeta)
	// 返回响应
	writer.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(writer).Encode(meta); err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
}

func main() {
	// 注册处理方法
	http.HandleFunc("/bitcask/put", handlePut)
	http.HandleFunc("/bitcask/get", handleGet)
	http.HandleFunc("/bitcask/delete", handleDelete)
	http.HandleFunc("/bitcask/listkeys", handleListKeys)
	http.HandleFunc("/bitcask/stat", handleStat)
	http.HandleFunc("/bitcask/prefix", handlePrefix) // 新增前缀查询路由
	http.HandleFunc("/memory/get", handleMemoryGet)
	http.HandleFunc("/memory/set", handleMemorySet)
	http.HandleFunc("/memory/search", handleMemorySearch)
	http.HandleFunc("/memory/create", handleCreateMemoryMeta)

	// 启动 HTTP 服务
	log.Println("Starting HTTP server on localhost:8080...")
	if err := http.ListenAndServe("localhost:8080", nil); err != nil {
		log.Fatalf("Failed to start HTTP server: %v\n", err)
	}
}
