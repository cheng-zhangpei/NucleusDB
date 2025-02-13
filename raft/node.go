package raft

import (
	"ComDB/raft/pb"
	"ComDB/raft/tracker"
	"context"
	"google.golang.org/grpc"
	"log"
	"net"
	"sync"
	"time"
)

// Node the bridge of application(ComDB) and raft module
type Node interface {
}

type Ready struct {
	Entries          []*pb.Entry   // 需要持久化的新日志条目
	CommittedEntries []*pb.Entry   // 已提交待应用的日志条目
	Messages         []*pb.Message // 需要发送给其他节点的消息
	HardState        HardState     // 需要持久化的硬状态（Term、Vote、Commit Index）
	SoftState        SoftState     // 软状态（Leader ID、节点角色）
}
type msgWithResult struct {
	m      *pb.Message
	result chan error
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
	client *RaftClient
}

type raftServer struct {
	pb.UnimplementedRaftServer
	node *node
}
type RaftClient struct {
	conn *grpc.ClientConn
	rpc  pb.RaftClient
}

func NewRaftClient(address string) *RaftClient {
	// 创建与 Raft 服务器的连接
	conn, err := grpc.NewClient(address, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	// 创建 Raft 客户端
	rpcClient := pb.NewRaftClient(conn)

	return &RaftClient{
		conn: conn,
		rpc:  rpcClient,
	}
}

func (s *raftServer) ProcessMessage(ctx context.Context, msg *pb.Message) (*pb.Message, error) {
	// 将接收到的消息发送到 recvc 通道
	s.node.recvc <- msg
	return &pb.Message{}, nil
}

func newReady(r *raft, prevSoftSt *SoftState, prevHardSt *HardState) *Ready {
	rd := &Ready{
		//
		Entries:          r.ms.ents,
		CommittedEntries: r.raftLog.nextEnts(),
		Messages:         r.msgs,
	}

	// 检查软状态是否发生变化
	if softSt := r.softState(); softSt != nil && prevSoftSt == nil {
		rd.SoftState = *softSt
	}
	// 检查硬状态是否发生变化
	if hardSt := r.hardState(); !isHardStateEqual(hardSt, prevHardSt) {
		rd.HardState = *hardSt
	}
	return rd
}

func (a *SoftState) equal(b *SoftState) bool {
	return a.Lead == b.Lead && a.RaftState == b.RaftState
}

func isHardStateEqual(a, b *HardState) bool {
	return a.Term == b.Term && a.Vote == b.Vote && a.Commit == b.Commit
}
func RestartNode(c *RaftConfig) Node {
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
	registerRPCServer(n, c.grpcServerAddr)
	// 开启监控缓冲区的信息发送进程
	n.sendMessage(c.sendInterval)
	// 开启接受消息主进程
	n.startTicker(c.tickInterval)
	go n.run()
	return &n
}
func registerRPCServer(n *node, addr string) {
	s := grpc.NewServer()
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	pb.RegisterRaftServer(s, &raftServer{node: n})
	go func() {
		err := s.Serve(lis)
		if err != nil {
			panic(err)
		}
	}()
}
func newNode(rn *RawNode, config *RaftConfig) *node {
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
		client: NewRaftClient(config.grpcClientAddr),
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
	var propc chan *msgWithResult
	var readyc chan *Ready
	var advancec chan struct{}
	var rd *Ready

	r := n.rn.raft

	lead := None
	for {
		if advancec != nil {
			readyc = nil
		} else if n.rn.HasReady() {
			rd = n.rn.readyWithoutAccept()
			readyc = n.readyc
		}
		// leader unmatched
		if lead != r.lead {
			// unmatched but have leader
			if r.lead != None {
				if lead == None {
					// 第一轮，这个时候还没有leader
					log.Println("raft.node: %x elected leader %x at term %d", r.id, r.lead, r.Term)
				} else {
					//
					log.Println("raft.node: %x changed leader from %x to %x at term %d", r.id, lead, r.lead, r.Term)
				}
				propc = n.propc
			} else { //两个leader不相同，说明这个时候已经改变leader了
				log.Println("raft.node: %x lost leader %x at term %d", r.id, lead, r.Term)
				propc = nil
			}
			lead = r.lead
		}

		select {
		case pm := <-propc:
			m := pm.m
			m.From = r.id
			err := r.Step(m)
			if pm.result != nil {
				pm.result <- err
				close(pm.result)
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
		case readyc <- rd:
			n.rn.acceptReady(rd)
			advancec = n.advancec
		case <-advancec:
			n.rn.Advance(rd)
			rd = &Ready{}
			advancec = nil
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
		log.Println("%x A tick missed to fire. Node blocks too long!", n.rn.raft.id)
	}
}

// sendMessage: 开一个线程, 将msg暂存区中的数据发送出去
func (n *node) sendMessages(sleepTime time.Duration) {
	// 启动一个 goroutine 来处理消息发送
	go func() {
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
				if err := n.sendAllCache(msg); err != nil {
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

func (n *node) sendAllCache(msgs []*pb.Message) error {
	for _, m := range msgs {
		if m != nil {
			// 发送消息到其他节点
			response, err := n.client.SendMessage(m)
			if err != nil {
				return err
			}
			// 将消息放入接收通道
			select {
			case n.recvc <- response:
				// 消息已成功放入通道
			default:
				// 通道已满，无法放入消息
				log.Println("recvc channel is full, message dropped")
			}
		}
	}
	return nil
}
func (n *node) startTicker(interval time.Duration) {
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

func (c *RaftClient) Close() error {
	return c.conn.Close()
}
func (c *RaftClient) SendMessage(msg *pb.Message) (*pb.Message, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return c.rpc.SendMessage(ctx, msg)
}
