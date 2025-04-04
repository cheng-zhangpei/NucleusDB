package txn

import (
	"github.com/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"log"
	_ "log"
	"testing"
	"time"
)

// TestZookeeperConn_Unit 单元测试（模拟测试）
func TestZookeeperConn_Unit(t *testing.T) {
	// 测试1：单纯测试构造函数
	t.Run("NewConnection", func(t *testing.T) {
		zc := NewZookeeperConn([]string{"127.0.0.1:2181"}, 5*time.Second, nil)
		assert.NotNil(t, zc)
		assert.False(t, zc.IsConnected())
	})

	// 测试2：测试无效连接
	t.Run("InvalidConnection", func(t *testing.T) {
		zc := NewZookeeperConn([]string{"invalid:2181"}, 100*time.Millisecond, nil)
		err := zc.Connect()
		assert.Error(t, err)
		assert.False(t, zc.IsConnected())
	})
}

// TestZookeeperConn_Integration 集成测试（需要真实ZK服务）
func TestZookeeperConn_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过集成测试")
	}

	// 1. 初始化连接
	zkServers := []string{"127.0.0.1:2181"} // 修改为你的ZK地址
	zc := NewZookeeperConn(zkServers, 5*time.Second, nil)
	log.Println(zc.Connect())
	require.NoError(t, zc.Connect())

	defer zc.Close()

	// 测试基础路径
	testPath := "/test_" + time.Now().Format("20060102150405")

	t.Run("BasicCRUD", func(t *testing.T) {
		// 2. 测试创建节点
		path, err := zc.Create(testPath, []byte("test_data"), 0)
		assert.NoError(t, err)
		assert.Equal(t, testPath, path)

		// 3. 测试读取节点
		data, stat, err := zc.Get(testPath)
		assert.NoError(t, err)
		assert.Equal(t, []byte("test_data"), data)

		// 4. 测试更新节点
		newStat, err := zc.Set(testPath, []byte("new_data"), stat.Version)
		assert.NoError(t, err)

		// 5. 验证更新结果
		data, _, err = zc.Get(testPath)
		assert.NoError(t, err)
		assert.Equal(t, []byte("new_data"), data)

		// 6. 测试删除节点
		assert.NoError(t, zc.Delete(testPath, newStat.Version))

		// 7. 验证节点不存在
		exists, _, err := zc.conn.Exists(testPath)
		assert.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("WatchNode", func(t *testing.T) {
		watchPath := testPath + "_watch"
		_, err := zc.Create(watchPath, nil, 0)
		require.NoError(t, err)

		// 1. 启动监听
		ch, err := zc.Watch(watchPath)
		require.NoError(t, err)

		// 2. 在协程中触发变更
		go func() {
			time.Sleep(100 * time.Millisecond)
			_, err := zc.Set(watchPath, []byte("changed"), -1)
			assert.NoError(t, err)
		}()

		// 3. 等待事件
		select {
		case event := <-ch:
			assert.Equal(t, zk.EventNodeDataChanged, event.Type)
		case <-time.After(1 * time.Second):
			t.Fatal("等待监听事件超时")
		}

		// 清理
		assert.NoError(t, zc.Delete(watchPath, -1))
	})

	t.Run("CreateRecursive", func(t *testing.T) {
		deepPath := testPath + "/deep/node/path"
		_, err := zc.Create(deepPath, []byte("deep_data"), 0)
		assert.NoError(t, err)

		// 验证各级节点存在
		exists, _, err := zc.conn.Exists(testPath + "/deep")
		assert.NoError(t, err)
		assert.True(t, exists)

		exists, _, err = zc.conn.Exists(testPath + "/deep/node")
		assert.NoError(t, err)
		assert.True(t, exists)

		// 清理
		assert.NoError(t, zc.Delete(deepPath, -1))
		assert.NoError(t, zc.Delete(testPath+"/deep/node", -1))
		assert.NoError(t, zc.Delete(testPath+"/deep", -1))
	})
}

// TestZookeeperConn_Reconnect 测试重连逻辑
func TestZookeeperConn_Reconnect(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过集成测试")
	}

	zkServers := []string{"127.0.0.1:2181"}
	zc := NewZookeeperConn(zkServers, 5*time.Second, nil)
	require.NoError(t, zc.Connect())
	defer zc.Close()

	testPath := "/reconnect_test_" + time.Now().Format("20060102150405")
	_, err := zc.Create(testPath, []byte("data"), 0)
	require.NoError(t, err)

	// 模拟连接中断
	zc.conn.Close()

	// 应自动恢复的操作
	data, _, err := zc.Get(testPath)
	assert.NoError(t, err)
	assert.Equal(t, []byte("data"), data)

	// 清理
	assert.NoError(t, zc.Delete(testPath, -1))
}
