package txn

import (
	"encoding/json"
	"log"
	"net/http"
)

// CoordinatorHTTP 封装HTTP服务器的协调者
type CoordinatorHTTP struct {
	*Coordinator
	server *http.Server
}

// NewCoordinatorHTTP 创建带HTTP服务的协调者
func NewCoordinatorHTTP(zkAddr, httpAddr string) {
	coordinator := NewCoordinator(zkAddr)

	// 创建HTTP服务器
	mux := http.NewServeMux()
	coordinatorHTTP := &CoordinatorHTTP{
		Coordinator: coordinator,
		server: &http.Server{
			Addr:    httpAddr,
			Handler: mux,
		},
	}

	// 注册路由处理函数
	mux.HandleFunc("/conflict-check", coordinatorHTTP.handleConflictCheckHTTP)
	mux.HandleFunc("/save-snapshot", coordinatorHTTP.handleSaveSnapshotHTTP)

	// 启动服务器
	log.Printf("coordinator http server have been started on %s\n", httpAddr)
	if err := coordinatorHTTP.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		panic("HTTP server failed: " + err.Error())
	}
}

// HTTP请求响应结构体
type conflictCheckRequest struct {
	CheckKeys  []uint64 `json:"checkKeys"`
	StartTime  uint64   `json:"startTime"`
	CommitTime uint64   `json:"commitTime"`
}

type conflictCheckResponse struct {
	HasConflict bool   `json:"hasConflict"`
	Error       string `json:"error,omitempty"`
}

type saveSnapshotRequest struct {
	Snapshot *TxnSnapshot `json:"snapshot"`
}

// HTTP处理函数
func (ch *CoordinatorHTTP) handleConflictCheckHTTP(w http.ResponseWriter, r *http.Request) {
	var req conflictCheckRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// 转换checkKeys为map
	checkKeyMap := make(map[uint64]struct{})
	for _, key := range req.CheckKeys {
		checkKeyMap[key] = struct{}{}
	}
	log.Printf("收到冲突检测请求[%d->%d]\n", req.StartTime, req.CommitTime)
	hasConflict, err := ch.handleConflictCheck(checkKeyMap, req.StartTime, req.CommitTime)
	resp := conflictCheckResponse{
		HasConflict: hasConflict,
	}
	if err != nil {
		resp.Error = err.Error()
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (ch *CoordinatorHTTP) handleSaveSnapshotHTTP(w http.ResponseWriter, r *http.Request) {
	var req saveSnapshotRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil { // 解析整个结构体
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := ch.saveSnapshot(req.Snapshot); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
