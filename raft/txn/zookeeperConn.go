package txn

import (
	"errors"
	"fmt"
	"github.com/go-zookeeper/zk"
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
}

// NewZookeeperConn 创建连接实例
func NewZookeeperConn(servers []string, timeout time.Duration, acl []zk.ACL) *zookeeperConn {
	if acl == nil {
		acl = zk.WorldACL(zk.PermAll)
	}
	return &zookeeperConn{
		servers: servers,
		timeout: timeout,
		acl:     acl,
	}
}

// ================= 连接管理 =================

func (zc *zookeeperConn) Connect() error {
	zc.mutex.Lock()
	defer zc.mutex.Unlock()

	if zc.connected {
		return nil
	}

	conn, eventChan, err := zk.Connect(zc.servers, zc.timeout, zk.WithLogInfo(false))
	if err != nil {
		return fmt.Errorf("ZK连接失败: %v", err)
	}

	// 增加更全面的状态检查
	for {
		select {
		case event := <-eventChan:
			switch event.State {
			case zk.StateConnected, zk.StateHasSession:
				zc.conn = conn
				zc.eventChan = eventChan
				zc.connected = true
				zc.sessionID = conn.SessionID()
				return nil
			case zk.StateExpired, zk.StateDisconnected:
				conn.Close()
				return fmt.Errorf("连接失败，最终状态: %v", event.State)
			}
			// 其他状态继续等待
		case <-time.After(zc.timeout):
			conn.Close()
			return errors.New("连接超时")
		}
	}
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

	if !zc.connected {
		return nil, errors.New("连接未建立")
	}

	return zc.conn.Set(path, data, version)
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

// GetByPrefix 获取指定前缀的所有键值对，key为时间戳(uint64)
func (zc *zookeeperConn) GetSnapshotByPrefix(prefix string) (map[uint64][]byte, error) {
	zc.mutex.Lock()
	defer zc.mutex.Unlock()

	if !zc.connected {
		return nil, errors.New("连接未建立")
	}

	// 获取所有子节点
	children, _, err := zc.conn.Children("/")
	if err != nil {
		return nil, fmt.Errorf("获取子节点失败: %v", err)
	}

	result := make(map[uint64][]byte)
	for _, child := range children {
		if !strings.HasPrefix(child, prefix) {
			continue
		}

		// 提取时间戳部分（假设格式为 "prefix-timestamp"）
		timestampStr := strings.TrimPrefix(child, prefix+"-")
		timestamp, err := strconv.ParseUint(timestampStr, 10, 64)
		if err != nil {
			continue // 跳过格式不正确的节点
		}

		data, _, err := zc.conn.Get("/" + child)
		if err != nil {
			return nil, fmt.Errorf("获取节点 %s 数据失败: %v", child, err)
		}

		result[timestamp] = data
	}
	return result, nil
}
