package permission

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_PermissingManager(t *testing.T) {
	manager := NewPermissionManager()
	// 创建一个权限对
	manager.GrantPermission("0", "space1", ReadOnly, 0)
	permission1 := manager.CheckPermission("0", "space1", PermissionRead)
	assert.Equal(t, permission1, true)

	permission2 := manager.CheckPermission("0", "space1", PermissionUpdate)
	assert.Equal(t, permission2, false)

	// 修改权限
	manager.GrantPermission("0", "space1", Admin, 0)
	permission3 := manager.CheckPermission("0", "space1", PermissionUpdate)
	assert.Equal(t, permission3, true)

	// 一个记忆空间的用户
	manager.GrantPermission("1", "space1", Admin, 0)
	manager.GrantPermission("2", "space1", Admin, 0)
	list := manager.ListMemSpaceAgents("space1")
	for item := range list {
		println(item)
	}
}
