package txn

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-zookeeper/zk"
	"path"
	_ "strconv"
	_ "strings"
	"sync"
	"time"
)

// 新增常量定义
const (
	TimestampLeaseBase  = "/timestamp_lease"
	GlobalPointerPath   = TimestampLeaseBase + "/global_pointer"
	ActiveLeasesPath    = TimestampLeaseBase + "/active_leases"
	DefaultLeaseBatch   = 1000            // 默认每次分配的租约数量
	LeaseRenewThreshold = 100             // 租约剩余数量达到该阈值时触发续期
	LeaseCheckInterval  = 5 * time.Second // 租约状态检查间隔
	LeaseRequestTimeout = 3 * time.Second // 租约请求超时时间
)

// 新增错误定义
var (
	ErrLeaseExhausted     = errors.New("current lease exhausted")
	ErrCASFailed          = errors.New("CAS operation failed")
	ErrLeaseNotHeld       = errors.New("lease not held by current session")
	ErrInvalidLeaseFormat = errors.New("invalid lease data format")
)

// LeaseInfo 租约信息结构体
type LeaseInfo struct {
	Start      uint64 `json:"start"`
	End        uint64 `json:"end"`
	SessionID  int64  `json:"session_id"`  // 关联的ZK会话ID
	LeaderName string `json:"leader_name"` // Leader标识
}

// TimestampLeaseManager 时间戳租约管理器
type TimestampLeaseManager struct {
	zc           *zookeeperConn
	currentLease *LeaseInfo
	leaderID     string
	stopChan     chan struct{}
	mu           sync.RWMutex
}

// NewTimestampLeaseManager 创建租约管理器
func NewTimestampLeaseManager(zc *zookeeperConn, leaderID string) *TimestampLeaseManager {
	return &TimestampLeaseManager{
		zc:       zc,
		leaderID: leaderID,
		stopChan: make(chan struct{}),
	}
}

// ================= 核心租约管理方法 =================

// AcquireLease 申请新的时间戳租约（阻塞式）
func (tm *TimestampLeaseManager) AcquireLease() (uint64, uint64, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	// 确保基础路径存在
	if err := tm.ensureBasePaths(); err != nil {
		return 0, 0, fmt.Errorf("初始化路径失败: %v", err)
	}

	// 使用CAS循环直到成功获取租约
	for {
		select {
		case <-tm.stopChan:
			return 0, 0, errors.New("租约获取已中止")
		default:
			// 获取当前全局指针
			currentPointer, version, err := tm.getGlobalPointer()
			if err != nil {
				time.Sleep(1 * time.Second)
				continue
			}

			// 计算新租约范围
			newStart := currentPointer + 1
			newEnd := newStart + DefaultLeaseBatch - 1

			// 尝试原子更新全局指针
			if ok, err := tm.casGlobalPointer(currentPointer, newEnd, version); err != nil {
				continue
			} else if ok {
				// 创建临时租约节点
				leasePath := path.Join(ActiveLeasesPath, tm.leaderID)
				leaseData, _ := json.Marshal(LeaseInfo{
					Start:      newStart,
					End:        newEnd,
					SessionID:  tm.zc.sessionID,
					LeaderName: tm.leaderID,
				})

				_, err := tm.zc.Create(leasePath, leaseData, zk.FlagEphemeral)
				if err != nil {
					// 回滚全局指针
					tm.rollbackGlobalPointer(newEnd, currentPointer)
					return 0, 0, fmt.Errorf("创建租约节点失败: %v", err)
				}

				tm.currentLease = &LeaseInfo{
					Start:      newStart,
					End:        newEnd,
					SessionID:  tm.zc.sessionID,
					LeaderName: tm.leaderID,
				}

				// 启动后台租约维护
				go tm.leaseMaintenanceLoop()
				return newStart, newEnd, nil
			}
			// CAS失败，等待后重试
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// RenewLease 续期租约（非阻塞式）
func (tm *TimestampLeaseManager) RenewLease() error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if tm.currentLease == nil {
		return ErrLeaseNotHeld
	}

	// 获取当前全局指针
	currentPointer, version, err := tm.getGlobalPointer()
	if err != nil {
		return err
	}

	// 必须基于当前租约的end值续期
	if currentPointer != tm.currentLease.End {
		return fmt.Errorf("租约状态不一致，当前全局指针:%d 本地租约结束:%d",
			currentPointer, tm.currentLease.End)
	}

	newEnd := currentPointer + DefaultLeaseBatch
	if ok, err := tm.casGlobalPointer(currentPointer, newEnd, version); err != nil {
		return err
	} else if !ok {
		return ErrCASFailed
	}

	// 更新本地租约信息
	tm.currentLease.End = newEnd

	// 更新临时节点数据
	leasePath := path.Join(ActiveLeasesPath, tm.leaderID)
	leaseData, _ := json.Marshal(tm.currentLease)
	_, err = tm.zc.Set(leasePath, leaseData, -1)
	return err
}

// ReleaseLease 主动释放租约
func (tm *TimestampLeaseManager) ReleaseLease() error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if tm.currentLease == nil {
		return nil
	}

	leasePath := path.Join(ActiveLeasesPath, tm.leaderID)
	if err := tm.zc.Delete(leasePath, -1); err != nil && err != zk.ErrNoNode {
		return err
	}

	tm.currentLease = nil
	close(tm.stopChan)
	return nil
}

// GetTimestamp 从本地租约分配时间戳
func (tm *TimestampLeaseManager) GetTimestamp() (uint64, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	if tm.currentLease == nil {
		return 0, ErrLeaseNotHeld
	}

	if tm.currentLease.Start > tm.currentLease.End {
		return 0, ErrLeaseExhausted
	}

	ts := tm.currentLease.Start
	tm.currentLease.Start++
	return ts, nil
}

// ================= 辅助方法 =================

// ensureBasePaths 确保基础路径存在
func (tm *TimestampLeaseManager) ensureBasePaths() error {
	if _, err := tm.zc.Create(TimestampLeaseBase, nil, 0); err != nil && err != zk.ErrNodeExists {
		return err
	}
	if _, err := tm.zc.Create(ActiveLeasesPath, nil, 0); err != nil && err != zk.ErrNodeExists {
		return err
	}
	// 初始化全局指针节点
	exists, _, err := tm.zc.conn.Exists(GlobalPointerPath)
	if err != nil {
		return err
	}
	if !exists {
		initData := make([]byte, 8)
		binary.BigEndian.PutUint64(initData, 0)
		if _, err := tm.zc.Create(GlobalPointerPath, initData, 0); err != nil && err != zk.ErrNodeExists {
			return err
		}
	}
	return nil
}

// getGlobalPointer 获取当前全局指针值和版本号
func (tm *TimestampLeaseManager) getGlobalPointer() (uint64, int32, error) {
	data, stat, err := tm.zc.Get(GlobalPointerPath)
	if err != nil {
		return 0, -1, err
	}
	if len(data) != 8 {
		return 0, -1, fmt.Errorf("invalid global pointer data")
	}
	return binary.BigEndian.Uint64(data), stat.Version, nil
}

// casGlobalPointer Compare-and-Swap更新全局指针
func (tm *TimestampLeaseManager) casGlobalPointer(oldVal, newVal uint64, version int32) (bool, error) {
	newData := make([]byte, 8)
	binary.BigEndian.PutUint64(newData, newVal)

	stat, err := tm.zc.Set(GlobalPointerPath, newData, version)
	if err == zk.ErrBadVersion {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return stat.Version == version+1, nil
}

// rollbackGlobalPointer 回滚全局指针（仅用于错误恢复）
func (tm *TimestampLeaseManager) rollbackGlobalPointer(current, target uint64) {
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, target)

	// 循环尝试直到成功或超时
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			_, ver, err := tm.getGlobalPointer()
			if err != nil {
				return
			}
			if _, err := tm.zc.Set(GlobalPointerPath, data, ver); err == nil {
				return
			}
		}
	}
}

// ================= 后台维护 =================

// leaseMaintenanceLoop 租约维护循环
func (tm *TimestampLeaseManager) leaseMaintenanceLoop() {
	ticker := time.NewTicker(LeaseCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			tm.checkLeaseStatus()
		case <-tm.stopChan:
			return
		}
	}
}

// checkLeaseStatus 检查租约状态
func (tm *TimestampLeaseManager) checkLeaseStatus() {
	tm.mu.RLock()
	currentLease := tm.currentLease
	tm.mu.RUnlock()

	if currentLease == nil {
		return
	}

	// 检查剩余量是否低于阈值
	remaining := currentLease.End - currentLease.Start + 1
	if remaining <= LeaseRenewThreshold {
		if err := tm.RenewLease(); err != nil {
			fmt.Printf("租约续期失败: %v\n", err)
		}
	}

	// 验证租约节点是否存在
	leasePath := path.Join(ActiveLeasesPath, tm.leaderID)
	exists, _, err := tm.zc.conn.Exists(leasePath)
	if err != nil || !exists {
		fmt.Println("租约节点丢失，触发重新获取")
		tm.ReleaseLease()
		tm.AcquireLease()
	}
}

// ================= 工具方法 =================

// GetAllActiveLeases 获取所有活跃租约（调试用）
func (tm *TimestampLeaseManager) GetAllActiveLeases() (map[string]LeaseInfo, error) {
	children, _, err := tm.zc.conn.Children(ActiveLeasesPath)
	if err != nil {
		return nil, err
	}

	leases := make(map[string]LeaseInfo)
	for _, child := range children {
		data, _, err := tm.zc.conn.Get(path.Join(ActiveLeasesPath, child))
		if err != nil {
			continue
		}

		var lease LeaseInfo
		if err := json.Unmarshal(data, &lease); err == nil {
			leases[child] = lease
		}
	}
	return leases, nil
}

// GetGlobalTimestamp 获取当前全局最大时间戳（只读）
func (tm *TimestampLeaseManager) GetGlobalTimestamp() (uint64, error) {
	val, _, err := tm.getGlobalPointer()
	return val, err
}
