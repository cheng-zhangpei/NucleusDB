package raft

import (
	"ComDB"
	"ComDB/raft/pb"
	"ComDB/raft/tracker"
	"context"
	"encoding/json"
	"google.golang.org/grpc"
	"log"
	"net"
	"net/http"
	"os"
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
	// DB instance
	DB *ComDB.DB
}

type raftServer struct {
	pb.UnimplementedRaftServer
	node *node
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

func (s *raftServer) ProcessMessage(ctx context.Context, msg *pb.Message) (*pb.Message, error) {
	// 将接收到的消息发送到 recvc 通道
	s.node.recvc <- msg
	return &pb.Message{}, nil
}

func StartNode(c *RaftConfig) {
	rn, err := NewRawNode(c)
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
	// 开启接受消息主进程
	startTicker(c.TickInterval, n)
	// start a thread to create a httpserver to trigger the raft module
	go startRaftHttpServer(n, c.HttpServerAddr)
	go run(n)
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

func newNode(rn *RawNode, config *RaftConfig) *node {
	opts := ComDB.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.DirPath = dir
	db, err := ComDB.Open(opts)
	if err != nil {
		panic(err)
	}
	return &node{
		propc:    make(chan *msgWithResult),
		recvc:    make(chan *pb.Message),
		readyc:   make(chan *Ready),
		advancec: make(chan struct{}),
		// make tickc a buffered chan, so raft node can buffer some ticks when the node
		// is busy processing raft messages. Raft node will resume process buffered
		// ticks when it becomes idle.
		tickc:  make(chan struct{}, 128),
		done:   make(chan struct{}),
		stop:   make(chan struct{}),
		status: make(chan chan *BaseStatus),
		rn:     rn,
		client: NewRaftClient(config.GRPCClientAddr, config.GRPCServerAddr),
		DB:     db,
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

func run(n *node) {
	var propc chan *msgWithResult
	//var readyc chan *Ready
	////var advancec chan struct{}
	//var rd *Ready

	r := n.rn.raft

	lead := None
	log.Printf("node initialization over,raft id: %s node start !\n", n.rn.raft.id)
	for {
		//if advancec != nil {
		//	readyc = nil
		//} else if n.rn.HasReady() {
		//	rd = n.rn.readyWithoutAccept()
		//	readyc = n.readyc
		//}
		// leader unmatched
		if lead != r.lead {
			// unmatched but have leader
			if r.lead != None {
				if lead == None {
					// 第一轮，这个时候还没有leader
					log.Printf("raft.node: %x elected leader %x at term %d\n", r.id, r.lead, r.Term)
				} else {
					//
					log.Printf("raft.node: %x changed leader from %x to %x at term %d\n", r.id, lead, r.lead, r.Term)
				}
				propc = n.propc
			} else { //两个leader不相同，说明这个时候已经改变leader了
				log.Printf("raft.node: %x lost leader %x at term %d\n", r.id, lead, r.Term)
				propc = nil
			}
			lead = r.lead
		}

		select {
		case pm := <-propc:
			// 这里会来delete请求和写请求
			if r.state != StateLeader {
				// 非 Leader 节点拒绝写请求
				continue
			}
			// 构造日志条目
			ent := pb.Entry{
				Term:  r.Term,
				Index: r.raftLog.lastIndex() + 1,
				Data:  pm.entryDetails,
			}
			// 追加到 Leader 日志
			if r.appendEntry(ent) {
				r.bcastAppend() // 广播给其他节点
			}
		// response from other raft node
		case m := <-n.recvc:
			// filter out response message from unknown From.
			if pr := r.processTracker.Progress[m.From]; pr != nil || !IsResponseMsg(m.Type) {
				r.Step(m)
			}
		// tick message from other node
		case <-n.tickc:
			n.rn.Tick()
		//case readyc <- rd:
		//n.rn.acceptReady(rd)
		//advancec = n.advancec
		//case <-advancec:
		//	n.rn.Advance(rd)
		//	rd = &Ready{}
		//	advancec = nil
		case c := <-n.status:
			c <- getStatus(r)
		case <-n.stop:
			close(n.done)
			return
		}
	}
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

// sendMessage: 开一个线程, 将msg暂存区中的数据发送出去
func sendMessages(sleepTime time.Duration, n *node) {
	// 启动一个 goroutine 来处理消息发送
	go func() {
		log.Printf("start sending messages go routine......\n")
		// 创建一个互斥锁，以确保线程安全地访问暂存区
		var mutex sync.Mutex
		// 创建一个通道，用于通知主线程消息发送完成
		done := make(chan struct{})

		for {
			// 锁定互斥锁，确保线程安全地访问暂存区
			mutex.Lock()
			// 检查暂存区是否有消息
			if len(n.rn.raft.msgs) > 0 {
				// 获取暂存区中的消息
				msg := n.rn.raft.msgs
				// 更新暂存区，移除已发送的消息
				// 解锁互斥锁
				mutex.Unlock()

				// 发送消息的逻辑
				if err := sendAllCache(msg, n); err != nil {
					log.Printf("Error sending message: %v", err)
					continue
				} else {
					log.Printf("Message sent: %+v", msg)
					// 清空暂存区
					mutex.Lock()
					n.rn.raft.msgs = make([]*pb.Message, 0)
					mutex.Unlock()
				}
			} else {
				// 如果暂存区为空，解锁互斥锁并继续等待
				mutex.Unlock()
				// 根据传入的睡眠时间进行等待，避免 CPU 过高占用
				time.Sleep(time.Millisecond * sleepTime)
			}

			// 检查是否所有消息都已发送完成
			if len(n.rn.raft.msgs) == 0 {
				// 通知主线程消息发送完成
				done <- struct{}{}
				break
			}
		}

		// 等待主线程接收完成通知
		<-done
	}()
}

func sendAllCache(msgs []*pb.Message, n *node) error {
	for _, m := range msgs {
		if m != nil {
			// 发送消息到其他节点
			for _, cl := range n.client {
				log.Printf("send msg to %d\n", m.To)
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
	}
	return nil
}
func startTicker(interval time.Duration, n *node) {
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
		log.Printf("start ticker go routine......\n")
	}()
}

func (c *RaftClient) Close() error {
	return c.conn.Close()
}

func (c *RaftClient) SendMessage(msg *pb.Message) (*pb.Message, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return c.rpc.SendMessage(ctx, msg)
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
	// only leader can put message
	if n.rn.raft.state != StateLeader {
		return
	}
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

	value, err := n.DB.Get([]byte(key))
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
	if request.Method != http.MethodDelete {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
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

// compiler seem have some problems
func startRaftHttpServer(n *node, addr string) {
	http.HandleFunc("/raft/put", n.handleRaftPut)
	http.HandleFunc("/raft/get", n.handleRaftGet)
	http.HandleFunc("/raft/delete", n.handleRaftDelete)

	// 启动 HTTP 服务器
	log.Printf("Starting Raft HTTP server on :%s\n", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("Failed to start HTTP server: %v", err)
	}
}
