package raft

import (
	"ComDB"
	"ComDB/raft/pb"
	"ComDB/raft/tracker"
	"ComDB/search"
	"context"
	"encoding/json"
	"fmt"
	"google.golang.org/grpc"
	"log"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type msgWithResult struct {
	entryDetails []byte
	httpType     HttpType
}
type HttpType int

const (
	// 定义 HTTP 类型的枚举值
	HttpTypeDelete HttpType = iota // iota 自动生成 0
	HttpTypePut                    // iota 自动生成 2
)

// 定义一个映射表，用于存储枚举值的描述
var httpTypeDescriptions = map[HttpType]string{
	HttpTypeDelete: "HTTP Delete 请求",
	HttpTypePut:    "HTTP PUT 请求",
}

type Status struct {
	BaseStatus
	Progress map[uint64]tracker.Progress
}

// todo 需要给node加一个事务处理的专门通道，注意这个通道也需要appendEntry，需要使用一套新的日志格式
type node struct {
	// propc 是一个消息通道，用于提交提案并接收提案处理结果。
	propc chan *msgWithResult
	// recvc 是一个消息通道，用于接收来自其他 Raft 节点的消息。
	recvc chan *pb.Message
	// readyc 是一个就绪通道，当 Raft 节点有新的日志条目或快照可提交时，会通过此通道通知上层应用。
	readyc chan *Ready
	// advancec 是一个通道，用于通知 Raft 节点上层应用已经应用了某些日志条目或快照。
	advancec chan struct{}
	// tickc 是一个滴答通道，用于触发 Raft 节点的定时器。
	tickc chan struct{}
	// 事务提案通道
	txnc chan struct{}
	// done 是一个完成通道，用于通知 Raft 节点已经完成某些操作。
	done chan struct{}
	// stop 是一个停止通道，用于通知 Raft 节点停止运行。
	stop chan struct{}
	// status 是一个状态通道，用于查询 Raft 节点的当前状态。
	status chan chan *BaseStatus
	// raft 是底层的 Raft 状态机，负责管理节点的日志复制和状态转换。
	rn *RawNode
	// use it for send message
	client []*RaftClient
}

type raftServer struct {
	pb.UnimplementedRaftServer
	node *node
}

func StartNode(c *RaftConfig, options ComDB.Options) {
	rn, err := NewRawNode(c, options)
	if err != nil {
		panic(err)
	}
	//err = rn.Bootstrap(peers)
	//if err != nil {
	//	c.Logger.Warningf("error occurred during starting a new node: %v", err)
	//}
	n := newNode(rn, c)
	// 启动 gRPC 服务器
	registerRPCServer(n, c.GRPCServerAddr)
	// 开启监控缓冲区的信息发送进程
	sendMessages(c.SendInterval, n)
	// 开启逻辑计时器
	startTicker(c.TickInterval, n)
	// 开启接受消息主进程
	// todo 此处还需要设计一个定时apply日志以及持久化日志的GoRoutine,但是这个具体的机制的确是需要思考的
	// todo 不需要按照etcd的ready和advance来进行实现,能不能设计一个更加个性化一些的内容
	n.run()
	// start a thread to create a httpserver to trigger the raft module
	// 此处的设计是为了让多个数据库实例在一个应用中运行
	go startRaftHttpServer(n, c.HttpServerAddr)
}
func StartNodeSeperated(c *RaftConfig, options ComDB.Options) {
	rn, err := NewRawNode(c, options)
	if err != nil {
		panic(err)
	}
	//err = rn.Bootstrap(peers)
	//if err != nil {
	//	c.Logger.Warningf("error occurred during starting a new node: %v", err)
	//}
	n := newNode(rn, c)
	// 启动 gRPC 服务器
	registerRPCServer(n, c.GRPCServerAddr)

	// 开启逻辑计时器
	startTicker(c.TickInterval, n)

	// 开启监控缓冲区的信息发送进程
	sendMessages(c.SendInterval, n)

	// 开启接受消息主进程
	n.run()
	// 此处修改使得主进程被阻塞，这个设计为容器专用，使得分布式数据库实例可以独立的在单个的容器中运行
	startRaftHttpServer(n, c.HttpServerAddr)
}

func newNode(rn *RawNode, config *RaftConfig) *node {
	return &node{
		propc:    make(chan *msgWithResult, 10),
		txnc:     make(chan struct{}, 10),
		recvc:    make(chan *pb.Message, 1024),
		readyc:   make(chan *Ready),
		advancec: make(chan struct{}),
		// make tickc a buffered chan, so raft node can bFuffer some ticks when the node
		// is busy processing raft messages. Raft node will resume process buffered
		// ticks when it becomes idle.
		tickc:  make(chan struct{}, 1024),
		done:   make(chan struct{}),
		stop:   make(chan struct{}),
		status: make(chan chan *BaseStatus),
		rn:     rn,
		client: NewRaftClient(config.GRPCClientAddr, config.GRPCServerAddr),
	}
}

func (n *node) Stop() {
	select {
	case n.stop <- struct{}{}:
		// Not already stopped, so trigger it
	case <-n.done:
		// Node has already been stopped - no need to do anything
		return
	}
	// Block until the stop has been acknowledged by run()
	<-n.done
}

func (n *node) run() {
	log.Printf("Node initialization over, Raft ID: %d, node start!\n", n.rn.raft.id)
	// 启动 Raft 主逻辑的 Goroutine
	go func() {
		var propc chan *msgWithResult
		var txnc chan struct{}
		r := n.rn.raft
		lead := None

		for {
			// 检查 leader 变化
			if lead != r.lead {
				if r.lead != None {
					if lead == None {
						log.Printf("raft.node: %x elected leader %x at term %d\n", r.id, r.lead, r.Term)
					} else {
						log.Printf("raft.node: %x changed leader from %x to %x at term %d\n", r.id, lead, r.lead, r.Term)
					}
					propc = n.propc
					txnc = n.txnc
				} else {
					log.Printf("raft.node: %x lost leader %x at term %d\n", r.id, lead, r.Term)
					propc = nil
					txnc = nil
				}
				lead = r.lead
			}

			select {
			case pm := <-propc:
				// 处理提案请求
				if r.state != StateLeader {
					log.Printf("raft.node: %x rejected proposal because it is not the leader\n", r.id)
					continue
				}
				// 构造日志条目,每次传输的日志条目必须不为空，也就是一定是有操作的日志
				if len(pm.entryDetails) != 0 {
					// 记住一下日志与message是处于不同的位置的
					ent := &pb.Entry{
						Term:  r.Term,
						Index: r.raftLog.lastIndex(),
						Data:  pm.entryDetails,
					}
					ents := make([]*pb.Entry, 1)
					ents[0] = ent
					m := &pb.Message{
						To:      r.id,
						Index:   r.raftLog.lastIndex(),
						Entries: ents,
						Type:    pb.MessageType_MsgProp,
					}
					r.Step(m)
				}

			case <-txnc:
				// 只有leader可以接受事务请求
				if r.state != StateLeader {
					log.Printf("raft.node: %x rejected txn prop because it is not the leader\n", r.id)
					continue
				}
				ent := &pb.Entry{
					Term:  r.Term,
					Index: r.raftLog.lastIndex(),
					Data:  nil,
				}
				ents := make([]*pb.Entry, 1)
				ents[0] = ent
				m := &pb.Message{
					To:      r.id,
					Index:   r.raftLog.lastIndex(),
					Entries: ents,
					Type:    pb.MessageType_MsgCommitTxn,
				}
				r.Step(m)
			case m := <-n.recvc:
				// 处理来自其他节点的消息
				//if pr := r.processTracker.Progress[m.From]; pr != nil || !IsResponseMsg(m.Type) {
				//	r.Step(m)
				//}
				// 判断信息是否是属于这个节点的
				if m.To == r.id {
					if m.Type != pb.MessageType_MsgHeartbeatResp && m.Type != pb.MessageType_MsgHeartbeat {
						log.Printf("Raft Node %d receive message[%+v] from node %d", n.rn.raft.id, m, m.From)
					}
					r.Step(m)
				}

			case <-n.tickc:
				// 处理心跳
				n.rn.Tick()

			case c := <-n.status:
				// 返回状态信息
				c <- getStatus(r)

			case <-n.stop:
				// 接收到停止信号
				log.Printf("raft.node: %x stopping\n", r.id)
				close(n.done)
				return
			}
		}
	}()
}

// Tick increments the internal logical clock for this Node. Election timeouts
// and heartbeat timeouts are in units of ticks.
func (n *node) Tick() {
	select {
	case n.tickc <- struct{}{}:
	case <-n.done:
	default:
		log.Printf("%x A tick missed to fire. Node blocks too long!\n", n.rn.raft.id)
	}
}
func sendMessages(sleepTime time.Duration, n *node) {
	// 启动一个 goroutine 来处理消息发送
	go func() {
		log.Printf("[Raft Node %d]start sending messages go routine......\n", n.rn.raft.id)
		// 创建一个互斥锁，以确保线程安全地访问暂存区
		var mutex sync.Mutex
		for {
			// 锁定互斥锁，确保线程安全地访问暂存区
			mutex.Lock()
			// 检查暂存区是否有消息
			if len(n.rn.raft.msgs) > 0 {
				// 获取暂存区中的消息
				msg := n.rn.raft.msgs
				// 更新暂存区，移除已发送的消息
				n.rn.raft.msgs = make([]*pb.Message, 0)
				// 解锁互斥锁
				mutex.Unlock()
				// 发送消息的逻辑
				if err := sendAllCache(msg, n); err != nil {
					log.Printf("Error sending message: %v", err)
				}
			} else {
				// 如果暂存区为空，解锁互斥锁
				mutex.Unlock()

			}
			// todo 为了保证消息处理的及时性这里直接让调度器帮我们决定啥时候去调用这个发送信息的线程了
			// 根据传入的睡眠时间进行等待，避免 CPU 过高占用
			//time.Sleep(time.Millisecond * sleepTime)
			// 检查是否所有消息都已发送完成

		}
	}()
}

func sendAllCache(msgs []*pb.Message, n *node) error {
	for _, m := range msgs {
		if m != nil {
			if m.From == 0 || m.To == 0 {
				continue
			}
			// 发送消息到其他节点
			for _, cl := range n.client {
				response, err := cl.SendMessage(m)
				if err != nil {
					return err
				}
				// 将消息放入接收通道
				select {
				case n.recvc <- response:
					// 消息已成功放入通道
				default:
					// 通道已满，无法放入消息
					log.Printf("recvc channel is full, message dropped")
				}
			}
		}
		// 发送之后的数据需要从暂存区中清除
		//msgs[i] = nil // 将已发送的消息标记为 nil
	}
	//removeNilMessages(msgs)
	return nil
}
func startTicker(interval time.Duration, n *node) {
	log.Printf("Raft Node: %d start ticker go routine......\n", n.rn.raft.id)

	go func() {
		ticker := time.NewTicker(time.Millisecond * interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				select {
				case n.tickc <- struct{}{}:
				default:
				}
			case <-n.stop:
				return
			}
		}
	}()
}

// removeNilMessages 移除切片中的所有 nil 消息
func removeNilMessages(msgs []*pb.Message) []*pb.Message {
	result := []*pb.Message{}
	for _, m := range msgs {
		if m != nil {
			result = append(result, m)
		}
	}
	return result
}
func (c *RaftClient) Close() error {
	return c.conn.Close()
}

// 1. put  2. delete  3. get
func (n *node) handleRaftPut(writer http.ResponseWriter, request *http.Request) {
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
	leaderID := n.rn.raft.lead
	if n.rn.raft.state != StateLeader {
		// 只返回 leader 的 ID
		writer.Header().Set("Content-Type", "text/plain")
		writer.WriteHeader(http.StatusForbidden)
		fmt.Fprint(writer, leaderID)
		log.Printf("only leader can put value. Current leader ID: %d\n", leaderID)
		return
	}
	// 此处需要判断该事务暂存区中是否有数据

	for key, value := range kv {
		var logData = []byte("PUT " + key + " " + value)
		msg := &msgWithResult{
			entryDetails: logData,
			httpType:     HttpTypePut,
		}
		n.propc <- msg
		log.Printf("Key-value pair inserted successfully: key=%s, value=%s\n", key, value)
	}
}

// get
func (n *node) handleRaftGet(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := request.URL.Query().Get("key")
	log.Printf("Received GET request for key: %s\n", key)
	// 我都无语了开的数据库实例都不一样
	value, err := n.rn.raft.app.DB.Get([]byte(key))
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

func (n *node) handleRaftDelete(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// 将当前的leader信息返回
	leaderID := n.rn.raft.lead
	if n.rn.raft.state != StateLeader {
		// 只返回 leader 的 ID
		writer.Header().Set("Content-Type", "text/plain")
		writer.WriteHeader(http.StatusForbidden)
		fmt.Fprint(writer, leaderID)
		log.Printf("only leader can delete value. Current leader ID: %d\n", leaderID)
		return
	}
	key := request.URL.Query().Get("key")
	if key == "" {
		http.Error(writer, "Key is required", http.StatusBadRequest)
		log.Println("Failed to delete: key is missing")
		return
	}

	log.Printf("Received DELETE request for key: %s\n", key)
	var logData = []byte("DELETE " + key)
	msg := &msgWithResult{
		entryDetails: logData,
		httpType:     HttpTypeDelete,
	}
	n.propc <- msg
	writer.WriteHeader(http.StatusOK)
	writer.Write([]byte("Key deleted successfully"))
}
func (n *node) handleMessageSearch(writer http.ResponseWriter, request *http.Request) {
	// 可以被不同角色的节点使用
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
	results := make([]string, 1)
	opts := ComDB.DefaultCompressOptions
	for key, value := range kv {
		ms := &search.MemoryStructure{
			Db: n.rn.raft.app.DB,
		}
		// 调用 matchSearch 方法
		result, err := ms.MatchSearch(value, key, opts)
		if err != nil {
			log.Fatalln(err)
		}
		results = append(results, result)
	}
	_ = json.NewEncoder(writer).Encode(results)
}

func (n *node) handleMemSet(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	leaderID := n.rn.raft.lead
	if n.rn.raft.state != StateLeader {
		// 只返回 leader 的 ID
		writer.Header().Set("Content-Type", "text/plain")
		writer.WriteHeader(http.StatusForbidden)
		fmt.Fprint(writer, leaderID)
		log.Printf("only leader can delete value. Current leader ID: %d\n", leaderID)
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
	var logData = []byte("MemSet " + data.AgentId + " " + data.Value)
	msg := &msgWithResult{
		entryDetails: logData,
		httpType:     HttpTypeDelete,
	}
	n.propc <- msg
	writer.WriteHeader(http.StatusOK)
	writer.Write([]byte("memory set successfully"))
}

// 在分布式数据库中删除该Agent的记忆空间
func (n *node) handleMemoryDel(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	leaderID := n.rn.raft.lead
	if n.rn.raft.state != StateLeader {
		// 只返回 leader 的 ID
		writer.Header().Set("Content-Type", "text/plain")
		writer.WriteHeader(http.StatusForbidden)
		fmt.Fprint(writer, leaderID)
		log.Printf("only leader can delete value. Current leader ID: %d\n", leaderID)
		return
	}
	var data struct {
		AgentId string `json:"agentId"`
	}
	if err := json.NewDecoder(request.Body).Decode(&data); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	var logData = []byte("MemDel" + data.AgentId)
	msg := &msgWithResult{
		entryDetails: logData,
		httpType:     HttpTypeDelete,
	}
	n.propc <- msg
}

func (n *node) handleMemoryGet(writer http.ResponseWriter, request *http.Request) {
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
		Db: n.rn.raft.app.DB,
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

func (n *node) handleCompress(writer http.ResponseWriter, request *http.Request) {
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
	var logData = []byte("Compress " + data.AgentID + " " + data.Endpoint)
	msg := &msgWithResult{
		entryDetails: logData,
		httpType:     HttpTypeDelete,
	}
	n.propc <- msg
	writer.WriteHeader(http.StatusOK)
	writer.Write([]byte("compress successfully"))
}

func (n *node) handleCreateMeta(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var data struct {
		AgentId   string `json:"agentId"`
		TotalSize int64  `json:"totalSize"`
	}
	if err := json.NewDecoder(request.Body).Decode(&data); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	var logData = []byte("CreateMemoryMeta " + data.AgentId + " " + strconv.FormatInt(data.TotalSize, 10))
	msg := &msgWithResult{
		entryDetails: logData,
		httpType:     HttpTypeDelete,
	}
	n.propc <- msg
	writer.WriteHeader(http.StatusOK)
	writer.Write([]byte("memory space successfully"))
}

// =========================================后面是事务相关的handler============================================
// handleTxnPut 处理事务PUT
func (n *node) handleTxnPut(writer http.ResponseWriter, request *http.Request) {

	if request.Method != http.MethodPost {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if n.rn.raft.state != StateLeader {
		writer.Header().Set("Content-Type", "text/plain")
		writer.WriteHeader(http.StatusForbidden)  // 403 表示非 Leader
		fmt.Fprintf(writer, "%d", n.rn.raft.lead) // 返回 leaderID（uint64）
		log.Printf("Current node is not leader. Leader ID: %d\n", n.rn.raft.lead)
		return
	}
	var kv map[string]string

	if err := json.NewDecoder(request.Body).Decode(&kv); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		log.Printf("Failed to decode request body: %v\n", err)
		return
	}
	if n.IsTxnSnapshotEmpty() {
		startTime, err := n.rn.raft.TxnGetTimestamp()
		if err != nil {
			log.Fatalln("timestamp allocation failed", err)
			return
		}
		n.rn.raft.txnSnapshot.StartWatermark = startTime
	}
	leaderID := n.rn.raft.lead
	if n.rn.raft.state != StateLeader {
		// 只返回 leader 的 ID
		writer.Header().Set("Content-Type", "text/plain")
		writer.WriteHeader(http.StatusForbidden)
		fmt.Fprint(writer, leaderID)
		log.Printf("only leader can put value. Current leader ID: %d\n", leaderID)
		return
	}
	for key, value := range kv {
		// 这里直接将数据塞到raft结构的缓冲区中
		err := n.rn.raft.txnSnapshot.Put([]byte(key), []byte(value))
		if err != nil {
			return
		}
		log.Printf("Key-value pair inserted successfully: key=%s, value=%s\n", key, value)
	}
}

// 处理事务删除
func (n *node) handleTxnDelete(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if n.rn.raft.state != StateLeader {
		writer.Header().Set("Content-Type", "text/plain")
		writer.WriteHeader(http.StatusForbidden)  // 403 表示非 Leader
		fmt.Fprintf(writer, "%d", n.rn.raft.lead) // 返回 leaderID（uint64）
		log.Printf("Current node is not leader. Leader ID: %d\n", n.rn.raft.lead)
		return
	}
	// 将当前的leader信息返回
	leaderID := n.rn.raft.lead
	if n.rn.raft.state != StateLeader {
		// 只返回 leader 的 ID
		writer.Header().Set("Content-Type", "text/plain")
		writer.WriteHeader(http.StatusForbidden)
		fmt.Fprint(writer, leaderID)
		log.Printf("only leader can delete value. Current leader ID: %d\n", leaderID)
		return
	}

	key := request.URL.Query().Get("key")
	if key == "" {
		http.Error(writer, "Key is required", http.StatusBadRequest)
		log.Println("Failed to delete: key is missing")
		return
	}
	if n.IsTxnSnapshotEmpty() {
		startTime, err := n.rn.raft.TxnGetTimestamp()
		if err != nil {
			log.Fatalln("timestamp allocation failed", err)
			return
		}
		n.rn.raft.txnSnapshot.StartWatermark = startTime
	}
	err := n.rn.raft.txnSnapshot.Delete([]byte(key))
	if err != nil {
		return
	}

	writer.WriteHeader(http.StatusOK)
	writer.Write([]byte("Key deleted successfully"))
}

// 处理事务GET
// todo
func (n *node) handleTxnGet(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if n.rn.raft.state != StateLeader {
		writer.Header().Set("Content-Type", "text/plain")
		writer.WriteHeader(http.StatusForbidden)  // 403 表示非 Leader
		fmt.Fprintf(writer, "%d", n.rn.raft.lead) // 返回 leaderID（uint64）
		log.Printf("Current node is not leader. Leader ID: %d\n", n.rn.raft.lead)
		return
	}
	key := request.URL.Query().Get("key")
	if n.IsTxnSnapshotEmpty() {
		startTime, err := n.rn.raft.TxnGetTimestamp()
		if err != nil {
			log.Fatalln("timestamp allocation failed", err)
			return
		}
		n.rn.raft.txnSnapshot.StartWatermark = startTime
	}
	err := n.rn.raft.txnSnapshot.Get([]byte(key))
	if err != nil {
		return
	}
	writer.Header().Set("Content-Type", "application/json")
}

// 在node层面的commit需要由用户控制
func (n *node) handleTxnCommit(writer http.ResponseWriter, request *http.Request) {
	// 这里才是真正发送信号的地方
	n.txnc <- struct{}{}
}

// TxnGetResult 获得事务的GET结果
func (n *node) TxnGetResult(writer http.ResponseWriter, request *http.Request) {
	//todo 获得Get在事务中的结果，这里干脆也用通道来进行吧
}

// compiler seem have some problems
func startRaftHttpServer(n *node, addr string) {
	// todo 如何知道该节点到底是不是leader？ 动态获取集群Leader节点地址？
	log.Printf("Raft Node: %d Starting Raft HTTP server on %s\n", n.rn.raft.id, addr)
	// 不同节点的动态路由
	putRouter := fmt.Sprintf("/raft/%d/put", n.rn.raft.id)
	getRouter := fmt.Sprintf("/raft/%d/get", n.rn.raft.id)
	deleteRouter := fmt.Sprintf("/raft/%d/delete", n.rn.raft.id)
	// 下面是记忆管理的API
	MemGetRouter := fmt.Sprintf("/raft/%d/MemGet", n.rn.raft.id)
	MemSetRouter := fmt.Sprintf("/raft/%d/MemSet", n.rn.raft.id)
	CompressRouter := fmt.Sprintf("/raft/%d/Compress", n.rn.raft.id)
	MemorySearchRouter := fmt.Sprintf("/raft/%d/MemorySearch", n.rn.raft.id)
	MemDelRouter := fmt.Sprintf("/raft/%d/MemDel", n.rn.raft.id)
	MemCreateMetaRouter := fmt.Sprintf("/raft/%d/MemCreateMeta", n.rn.raft.id)
	// 下面是事务的router
	TxnGetRouter := fmt.Sprintf("/raft/%d/TxnGet", n.rn.raft.id)
	TxnSetRouter := fmt.Sprintf("/raft/%d/TxnSet", n.rn.raft.id)
	TxnDeleteRouter := fmt.Sprintf("/raft/%d/TxnDelete", n.rn.raft.id)
	TxnCommitRouter := fmt.Sprintf("/raft/%d/TxnCommit", n.rn.raft.id)
	TxnGetResultRouter := fmt.Sprintf("/raft/%d/TxnCommit", n.rn.raft.id)

	// 注册 HTTP 处理函数
	http.HandleFunc(putRouter, n.handleRaftPut)
	http.HandleFunc(getRouter, n.handleRaftGet)
	http.HandleFunc(deleteRouter, n.handleRaftDelete)
	http.HandleFunc(MemGetRouter, n.handleMemoryGet)
	http.HandleFunc(MemSetRouter, n.handleMemSet)
	http.HandleFunc(CompressRouter, n.handleCompress)
	http.HandleFunc(MemorySearchRouter, n.handleMessageSearch)
	http.HandleFunc(MemDelRouter, n.handleMemoryDel)
	http.HandleFunc(MemCreateMetaRouter, n.handleCreateMeta)
	http.HandleFunc(TxnGetRouter, n.handleTxnGet)
	http.HandleFunc(TxnSetRouter, n.handleTxnPut)
	http.HandleFunc(TxnDeleteRouter, n.handleTxnDelete)
	http.HandleFunc(TxnCommitRouter, n.handleTxnCommit)
	http.HandleFunc(TxnGetResultRouter, n.TxnGetResult)

	// 启动 HTTP 服务器,这里直接阻塞进程就ok了,后台的进程都在成功运行
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("Failed to start HTTP server: %v", err)
	}
}
func (s *raftServer) SendMessage(ctx context.Context, msg *pb.Message) (*pb.Message, error) {
	// 将接收到的消息发送到 recvc 通道
	s.node.recvc <- msg
	return &pb.Message{}, nil
}
func (c *RaftClient) SendMessage(msg *pb.Message) (*pb.Message, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return c.rpc.SendMessage(ctx, msg)
}

type RaftClient struct {
	conn *grpc.ClientConn
	rpc  pb.RaftClient
}

func NewRaftClient(addresses []string, serverAddr string) []*RaftClient {
	clients := []*RaftClient{}
	for _, addr := range addresses {
		if addr == serverAddr {
			continue
		}
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("Failed to connect to %s: %v", addr, err)
		}
		rpcClient := pb.NewRaftClient(conn)
		clients = append(clients, &RaftClient{
			conn: conn,
			rpc:  rpcClient,
		})
		log.Printf("start an peer client for %s\n", addr)
	}
	return clients
}
func registerRPCServer(n *node, addr string) {
	s := grpc.NewServer()
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	pb.RegisterRaftServer(s, &raftServer{node: n})
	go func() {
		log.Printf("start rpc server on %s\n", addr)
		err := s.Serve(lis)
		if err != nil {
			panic(err)
		}
	}()
}

// IsTxnSnapshotEmpty 判断当前事务是否处于初始化状态
func (n *node) IsTxnSnapshotEmpty() bool {
	// 1. 直接访问结构体字段（假设调用方已确保n.rn.raft非空）
	snapshot := n.rn.raft.txnSnapshot

	// 2. 检查所有可能存储数据的字段
	return len(snapshot.PendingWrite) == 0 &&
		len(snapshot.PendingReads) == 0 &&
		len(snapshot.ConflictKeys) == 0 &&
		len(snapshot.Operations) == 0 &&
		snapshot.StartWatermark == 0 &&
		snapshot.CommitTime == 0
}
