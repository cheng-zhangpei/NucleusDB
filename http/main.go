package main

import (
	ComDB "ComDB"
	"ComDB/search"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

var db *ComDB.DB
var compressor *search.Compressor

func init() {
	// 初始化 DB 实例
	var err error
	options := ComDB.DefaultOptions
	// todo 一定是允许用户自己配置数据库信息的，这个地方对用户暂时没有很友好
	options.DirPath = "/tmp/bitcask_http_server"
	db, err = ComDB.Open(options)
	if err != nil {
		panic(fmt.Sprintf("failed to open db: %v", err))
	}
	log.Println("Database created successfully at:", options.DirPath)
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

func handleMerge(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	log.Println("Received request to get database statistics")

	err := db.Merge()
	if err != nil {
		log.Printf("merge fail!\n")
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	}
	writer.Header().Set("Content-Type", "application/json")
	log.Printf("merge successfully\n")
	_ = json.NewEncoder(writer)
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
	log.Printf("the value: %s... has been insert", data.Value[0:5])

	// 返回成功响应
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode("OK")
}
func handleMemoryDelete(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 解析请求体
	var data struct {
		AgentId string `json:"agentId"`
	}
	if err := json.NewDecoder(request.Body).Decode(&data); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	// 创建 MemoryStructure 实例
	ms := &search.MemoryStructure{
		Db: db,
	}

	// 调用 MMDel 方法
	if err, _ := ms.MMDel(data.AgentId); err != true {
		http.Error(writer, "delete error", http.StatusInternalServerError)
		return
	}
	return
}

// handleMemorySearch 处理搜索记忆的请求
func handleMemorySearch(writer http.ResponseWriter, request *http.Request) {
	opts := ComDB.DefaultCompressOptions
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
	result, err := ms.MatchSearch(searchItem, agentId, opts)

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
	log.Printf("the memory space of %s has been bulid", data.AgentId)
	writer.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(writer).Encode(meta); err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
}

// handleCompress 处理压缩请求
func handleCompress(writer http.ResponseWriter, request *http.Request) {
	opt := ComDB.DefaultCompressOptions
	if request.Method != http.MethodPost {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 解析请求体
	var data struct {
		AgentID  string `json:"agentId"`
		Endpoint string `json:"endpoint"`
	}
	if err := json.NewDecoder(request.Body).Decode(&data); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	// 注意一下需要单例获取，否则可能会导致内存膨胀而且运行效率很低
	if compressor == nil {
		// todo 这个一定要独立出去让用户创建compressor
		// 获取 Compressor 实例

		ms := &search.MemoryStructure{
			Db: db,
		}
		// 获取meta并装载=> 此时记忆空间装载完成
		metaData, err := ms.FindMetaData([]byte(data.AgentID))
		if err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
		}
		ms.Mm = metaData

		compressor, err = search.NewCompressor(opt, data.AgentID, ms) // 假设 NewCompressor 是 Compressor 的构造函数
		if err != nil {
			return
		}
	}
	// 调用 Compress 方法
	success, err := compressor.Compress(data.AgentID, data.Endpoint)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	// 返回响应
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(map[string]interface{}{
		"status":  "success",
		"success": success,
	})
}

func handleCreateCompressor(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// 解析请求体
	var data struct {
		AgentID  string `json:"agentId"`
		Endpoint string `json:"endpoint"`
	}
	opt := ComDB.DefaultCompressOptions
	if compressor == nil {
		ms := &search.MemoryStructure{
			Db: db,
		}
		// 获取meta并装载=> 此时记忆空间装载完成
		metaData, err := ms.FindMetaData([]byte(data.AgentID))
		if err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
		}
		ms.Mm = metaData
		compressor, err = search.NewCompressor(opt, data.AgentID, ms) // 假设 NewCompressor 是 Compressor 的构造函数
		if err != nil {
			return
		}
	}
	log.Printf("the memory space of %s has been bulid", data.AgentID)
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	return
}
func healthyCheck(writer http.ResponseWriter, request *http.Request) {
	// 设置响应头为JSON格式
	writer.Header().Set("Content-Type", "application/json")

	// 构造健康检查的响应内容
	response := map[string]string{
		"status":  "healthy",
		"message": "ComDB server is running",
	}

	// 将响应内容编码为JSON
	jsonResponse, err := json.Marshal(response)
	if err != nil {
		// 如果JSON编码失败，返回500内部服务器错误
		writer.WriteHeader(http.StatusInternalServerError)
		writer.Write([]byte(`{"status": "error", "message": "Failed to encode health check response"}`))
		return
	}

	// 返回200状态码和JSON响应
	writer.WriteHeader(http.StatusOK)
	_, err = writer.Write(jsonResponse)
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
	http.HandleFunc("/memory/delete", handleDelete)
	http.HandleFunc("/memory/create", handleCreateMemoryMeta)
	http.HandleFunc("/memory/compress", handleCompress)
	http.HandleFunc("/health", healthyCheck)
	http.HandleFunc("/memory/newCompressor", handleCreateCompressor)

	// 启动 HTTP 服务
	log.Println("Starting HTTP server on 172.31.88.128:9090...")
	if err := http.ListenAndServe("172.31.88.128:9090", nil); err != nil {
		log.Fatalf("Failed to start HTTP server: %v\n", err)
	}
}
