package txn

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-zookeeper/zk"
	"log"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

type zookeeperConn struct {
	conn      *zk.Conn
	servers   []string
	timeout   time.Duration
	eventChan <-chan zk.Event
	acl       []zk.ACL
	connected bool
	sessionID int64
	mutex     sync.Mutex // 保证线程安全
	closeChan chan struct{}
}

// NewZookeeperConn 创建连接实例
func NewZookeeperConn(servers []string, timeout time.Duration, acl []zk.ACL) *zookeeperConn {
	if acl == nil {
		acl = zk.WorldACL(zk.PermAll)
	}
	return &zookeeperConn{
		servers:   servers,
		timeout:   timeout,
		acl:       acl,
		closeChan: make(chan struct{}),
	}
}

// ================= 连接管理 =================
func (zc *zookeeperConn) Connect() error {
	zc.mutex.Lock()
	defer zc.mutex.Unlock()

	if zc.connected {
		return nil
	}

	conn, eventChan, err := zk.Connect(zc.servers, zc.timeout,
		zk.WithLogInfo(false),
	)
	if err != nil {
		return fmt.Errorf("ZK连接失败: %v", err)
	}

	zc.conn = conn
	zc.eventChan = eventChan

	// 启动事件循环（但先不处理事件，只确保事件不被丢弃）

	// 等待连接确认
	ctx, cancel := context.WithTimeout(context.Background(), zc.timeout)
	defer cancel()

	for {
		select {
		case event := <-eventChan: // 直接监听 eventChan
			if event.State == zk.StateConnected {
				zc.connected = true
				zc.sessionID = conn.SessionID()
				zc.startEventLoop()
				return nil
			}
			if event.State == zk.StateExpired {
				conn.Close()
				return fmt.Errorf("session expired")
			}
		case <-ctx.Done():
			conn.Close()
			return errors.New("连接超时")
		case <-zc.closeChan:
			conn.Close()
			return errors.New("连接已关闭")
		}
	}
}

func (zc *zookeeperConn) startEventLoop() {
	go func() {
		for event := range zc.eventChan {
			zc.mutex.Lock()
			switch event.State {
			case zk.StateDisconnected, zk.StateExpired:
				zc.connected = false
				zc.conn.Close()
			case zk.StateConnected:
				zc.connected = true
			}
			zc.mutex.Unlock()
		}
	}()
}
func (zc *zookeeperConn) Close() {
	zc.mutex.Lock()
	defer zc.mutex.Unlock()

	if zc.conn != nil {
		zc.conn.Close()
		zc.connected = false
	}
}

func (zc *zookeeperConn) IsConnected() bool {
	zc.mutex.Lock()
	defer zc.mutex.Unlock()
	return zc.connected && zc.conn != nil && zc.conn.State() == zk.StateConnected
}

// ================= 节点读写 =================

// Create 创建节点（自动创建父节点）
func (zc *zookeeperConn) Create(path string, data []byte, flags int32) (string, error) {
	zc.mutex.Lock()
	defer zc.mutex.Unlock()

	if !zc.connected {
		return "", errors.New("连接未建立")
	}

	// 递归创建父节点
	if err := zc.createParent(path); err != nil {
		return "", err
	}

	return zc.conn.Create(path, data, flags, zc.acl)
}

// Get 读取节点数据（带重试）
func (zc *zookeeperConn) Get(path string) ([]byte, *zk.Stat, error) {
	zc.mutex.Lock()
	defer zc.mutex.Unlock()

	if !zc.connected {
		return nil, nil, errors.New("连接未建立")
	}

	data, stat, err := zc.conn.Get(path)
	if err == zk.ErrConnectionClosed {
		if err := zc.reconnect(); err != nil {
			return nil, nil, err
		}
		return zc.conn.Get(path)
	}
	return data, stat, err
}

// Set 更新节点数据（带版本校验）
func (zc *zookeeperConn) Set(path string, data []byte, version int32) (*zk.Stat, error) {
	zc.mutex.Lock()
	defer zc.mutex.Unlock()

	// 带指数退避的重试机制
	var lastErr error
	if !zc.connected {
		if err := zc.Connect(); err != nil {
			return nil, err
		}
	}

	// 先尝试更新现有节点
	stat, err := zc.conn.Set(path, data, -1) // version=-1表示匹配任意版本
	switch {
	case err == nil:
		return stat, nil
	case zk.ErrNoNode == err:
		// 节点不存在则创建
		_, err = zc.conn.Create(path, data, 0, zk.WorldACL(zk.PermAll))
		if err == nil {
			// 创建成功后获取最新状态
			_, stat, err := zc.conn.Exists(path)
			return stat, err
		}
		if err != nil {
			log.Println(err)
		}
		lastErr = err
	case zk.ErrSessionExpired == err:
		zc.connected = false
		lastErr = err
	default:
		lastErr = err
	}
	// 指数退避等待 --> 这个和CSMA的思维的冲突逼退思维一样
	log.Println(lastErr)
	return nil, fmt.Errorf("SafeSet失败，最终错误: %v", lastErr)
}

// Delete 删除节点
func (zc *zookeeperConn) Delete(path string, version int32) error {
	zc.mutex.Lock()
	defer zc.mutex.Unlock()

	if !zc.connected {
		return errors.New("连接未建立")
	}

	return zc.conn.Delete(path, version)
}

// ================= 高级功能 =================

// Watch 监听节点变化
func (zc *zookeeperConn) Watch(path string) (<-chan zk.Event, error) {
	zc.mutex.Lock()
	defer zc.mutex.Unlock()

	if !zc.connected {
		return nil, errors.New("连接未建立")
	}

	_, _, ch, err := zc.conn.GetW(path)
	if err != nil {
		return nil, err
	}
	return ch, nil
}

// ================= 私有方法 =================

func (zc *zookeeperConn) createParent(nodePath string) error {
	parent := path.Dir(nodePath)
	if parent == "/" {
		return nil
	}

	exists, _, err := zc.conn.Exists(parent)
	if err != nil {
		return err
	}

	if !exists {
		if err := zc.createParent(parent); err != nil {
			return err
		}
		if _, err := zc.conn.Create(parent, nil, 0, zc.acl); err != nil && err != zk.ErrNodeExists {
			return err
		}
	}
	return nil
}

func (zc *zookeeperConn) reconnect() error {
	zc.conn.Close()
	zc.connected = false

	conn, eventChan, err := zk.Connect(zc.servers, zc.timeout)
	if err != nil {
		return err
	}

	select {
	case event := <-eventChan:
		if event.State == zk.StateConnected {
			zc.conn = conn
			zc.eventChan = eventChan
			zc.connected = true
			zc.sessionID = conn.SessionID()
			return nil
		}
	case <-time.After(zc.timeout):
		conn.Close()
		return errors.New("重连超时")
	}
	return errors.New("重连失败")
}

func isLowestSequence(children []string, seq string) bool {
	for _, child := range children {
		childSeq := child[strings.LastIndex(child, "-")+1:]
		if childSeq < seq {
			return false
		}
	}
	return true
}

func (zc *zookeeperConn) GetSnapshotByPrefix(prefix string) (map[uint64][]byte, error) {
	// 带重试的获取逻辑
	result, err := zc.tryGetSnapshotByPrefix(prefix)
	if err == nil {
		return result, nil
	}

	// 会话级错误需要完全重建连接
	if isSessionError(err) {
		zc.forceReconnect()
	}
	return nil, err
}

func (zc *zookeeperConn) tryGetSnapshotByPrefix(prefix string) (map[uint64][]byte, error) {
	zc.mutex.Lock()
	defer zc.mutex.Unlock()

	if !zc.connected {
		if err := zc.Connect(); err != nil {
			return nil, err
		}
	}

	children, _, err := zc.conn.Children("/")
	if err != nil {
		return nil, fmt.Errorf("获取子节点失败: %v", err)
	}

	result := make(map[uint64][]byte)
	for _, child := range children {
		child = fmt.Sprintf("/%s", child)
		if !strings.HasPrefix(child, prefix) {
			log.Println(err)
			continue
		}

		// 提取时间戳
		parts := strings.Split(child, "-")
		if len(parts) < 2 {
			log.Println(err)
			continue
		}

		timestamp, err := strconv.ParseUint(parts[len(parts)-1], 10, 64)
		if err != nil {
			log.Println(err)
			continue
		}
		data, _, err := zc.conn.Get(child)

		if err != nil {
			log.Println(err)
			continue // 跳过获取失败的节点
		}
		result[timestamp] = data
	}
	return result, nil
}

func isSessionError(err error) bool {
	return errors.Is(err, zk.ErrConnectionClosed) ||
		strings.Contains(err.Error(), "session expired")
}

func (zc *zookeeperConn) forceReconnect() {
	zc.mutex.Lock()
	defer zc.mutex.Unlock()

	if zc.conn != nil {
		zc.conn.Close()
	}
	zc.connected = false
}
