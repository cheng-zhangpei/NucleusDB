package memspace

import (
	"fmt"
	"testing"
)

// TestPermissionSystem 验证权限系统的位运算逻辑
func TestPermissionSystem(t *testing.T) {
	fmt.Println("=== 权限系统验证测试 ===")

	// 测试1：基础权限检查
	fmt.Println("1. 基础权限检查:")
	testBasicPermissions()

	// 测试2：权限组合
	fmt.Println("\n2. 权限组合测试:")
	testPermissionCombinations()

	// 测试3：权限操作
	fmt.Println("\n3. 权限操作测试:")
	testPermissionOperations()

	// 测试4：预定义权限集
	fmt.Println("\n4. 预定义权限集测试:")
	testPredefinedSets()

	// 测试5：边界情况
	fmt.Println("\n5. 边界情况测试:")
	testEdgeCases()
}

func testBasicPermissions() {
	// 测试单个权限
	fmt.Printf("ReadOnly: %b (二进制) -> %s\n", ReadOnly, ReadOnly.String())
	fmt.Printf("ReadWrite: %b (二进制) -> %s\n", ReadWrite, ReadWrite.String())
	fmt.Printf("Admin: %b (二进制) -> %s\n", Admin, Admin.String())

	// 验证权限检查
	fmt.Printf("ReadOnly 有读权限: %t\n", ReadOnly.HasPermission(PermissionRead))
	fmt.Printf("ReadOnly 有写权限: %t\n", ReadOnly.HasPermission(PermissionWrite))
	fmt.Printf("ReadWrite 有写权限: %t\n", ReadWrite.HasPermission(PermissionWrite))
	fmt.Printf("Admin 有管理员权限: %t\n", Admin.HasPermission(PermissionAdmin))
}

func testPermissionCombinations() {
	// 创建自定义权限组合
	var customPerms PermissionSet
	customPerms.AddPermission(PermissionRead)
	customPerms.AddPermission(PermissionWrite)
	customPerms.AddPermission(PermissionCompress)

	fmt.Printf("自定义权限: %b -> %s\n", customPerms, customPerms.String())
	fmt.Printf("  包含读权限: %t\n", customPerms.HasPermission(PermissionRead))
	fmt.Printf("  包含写权限: %t\n", customPerms.HasPermission(PermissionWrite))
	fmt.Printf("  包含压缩权限: %t\n", customPerms.HasPermission(PermissionCompress))
	fmt.Printf("  包含删除权限: %t\n", customPerms.HasPermission(PermissionDelete))

	// 测试权限移除
	customPerms.RemovePermission(PermissionWrite)
	fmt.Printf("移除写权限后: %b -> %s\n", customPerms, customPerms.String())
	fmt.Printf("  现在包含写权限: %t\n", customPerms.HasPermission(PermissionWrite))
}

func testPermissionOperations() {
	// 测试权限切片转换
	fullUserPerms := FullUser.ToSlice()
	fmt.Printf("FullUser 权限切片: ")
	for _, perm := range fullUserPerms {
		fmt.Printf("%s ", perm.String())
	}
	fmt.Println()

	// 测试权限添加和移除
	var dynamicPerms PermissionSet
	fmt.Printf("初始权限: %b -> %s\n", dynamicPerms, dynamicPerms.String())

	dynamicPerms.AddPermission(PermissionRead)
	fmt.Printf("添加读权限后: %b -> %s\n", dynamicPerms, dynamicPerms.String())

	dynamicPerms.AddPermission(PermissionWrite)
	fmt.Printf("添加写权限后: %b -> %s\n", dynamicPerms, dynamicPerms.String())

	dynamicPerms.AddPermission(PermissionDelete)
	fmt.Printf("添加删除权限后: %b -> %s\n", dynamicPerms, dynamicPerms.String())

	dynamicPerms.RemovePermission(PermissionWrite)
	fmt.Printf("移除写权限后: %b -> %s\n", dynamicPerms, dynamicPerms.String())
}

func testPredefinedSets() {
	tests := []struct {
		name     string
		permSet  PermissionSet
		expected []MemSpacePermission
	}{
		{"ReadOnly", ReadOnly, []MemSpacePermission{PermissionRead}},
		{"ReadWrite", ReadWrite, []MemSpacePermission{PermissionRead, PermissionWrite}},
		{"ReadUpdate", ReadUpdate, []MemSpacePermission{PermissionRead, PermissionUpdate}},
		{"FullUser", FullUser, []MemSpacePermission{PermissionRead, PermissionWrite, PermissionUpdate, PermissionDelete, PermissionCompress}},
		{"Admin", Admin, []MemSpacePermission{PermissionRead, PermissionWrite, PermissionUpdate, PermissionDelete, PermissionCompress, PermissionShare, PermissionAdmin}},
	}

	for _, test := range tests {
		fmt.Printf("%s: %b -> %s\n", test.name, test.permSet, test.permSet.String())

		// 验证每个期望的权限都存在
		for _, expectedPerm := range test.expected {
			if !test.permSet.HasPermission(expectedPerm) {
				fmt.Printf("  ❌ 缺失权限: %s\n", expectedPerm.String())
			}
		}

		// 验证没有多余的权限
		actualPerms := test.permSet.ToSlice()
		if len(actualPerms) != len(test.expected) {
			fmt.Printf("  ⚠️  权限数量不匹配: 期望%d, 实际%d\n", len(test.expected), len(actualPerms))
		}
	}
}

func testEdgeCases() {
	// 测试空权限
	var emptyPerms PermissionSet
	fmt.Printf("空权限: %b -> %s\n", emptyPerms, emptyPerms.String())
	fmt.Printf("  空权限有读权限: %t\n", emptyPerms.HasPermission(PermissionRead))
	fmt.Printf("  空权限转换为切片: %v\n", emptyPerms.ToSlice())

	// 测试重复添加同一权限
	var dupPerms PermissionSet
	dupPerms.AddPermission(PermissionRead)
	dupPerms.AddPermission(PermissionRead) // 重复添加
	fmt.Printf("重复添加读权限后: %b -> %s\n", dupPerms, dupPerms.String())

	// 测试移除不存在的权限
	dupPerms.RemovePermission(PermissionWrite) // 移除不存在的权限
	fmt.Printf("移除不存在的写权限后: %b -> %s\n", dupPerms, dupPerms.String())

	// 测试权限枚举的字符串表示
	fmt.Printf("权限枚举字符串: ")
	for i := PermissionNone; i <= PermissionAdmin; i++ {
		fmt.Printf("%s(%d) ", i.String(), i)
	}
	fmt.Println()
}

// 单元测试版本（如果你使用go test）
func TestPermissions(t *testing.T) {
	// 测试权限检查
	if !ReadOnly.HasPermission(PermissionRead) {
		t.Error("ReadOnly 应该包含读权限")
	}

	if ReadOnly.HasPermission(PermissionWrite) {
		t.Error("ReadOnly 不应该包含写权限")
	}

	// 测试权限添加
	var perms PermissionSet
	perms.AddPermission(PermissionRead)
	if !perms.HasPermission(PermissionRead) {
		t.Error("添加读权限后应该包含读权限")
	}

	// 测试权限移除
	perms.RemovePermission(PermissionRead)
	if perms.HasPermission(PermissionRead) {
		t.Error("移除读权限后不应该包含读权限")
	}

	// 测试预定义权限集
	if !FullUser.HasPermission(PermissionRead) {
		t.Error("FullUser 应该包含读权限")
	}
	if !FullUser.HasPermission(PermissionWrite) {
		t.Error("FullUser 应该包含写权限")
	}
	if !Admin.HasPermission(PermissionAdmin) {
		t.Error("Admin 应该包含管理员权限")
	}

	fmt.Println("所有权限测试通过! ✅")
}
