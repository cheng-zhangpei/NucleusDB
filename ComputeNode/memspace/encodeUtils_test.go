package memspace

import (
	"testing"
)

func TestEncodeDecodeMMMeta(t *testing.T) {
	// 创建测试数据
	original := NewMemMetaData(1, Private, 1<<20)
	original.MemSpaceId = 987654321 // 新增字段
	original.BindingAgents = []uint64{111, 222, 333, 444, 555}
	spaceType := Shared
	spaceStatus := Binding
	original.SpaceType = spaceType
	original.SpaceStatus = spaceStatus
	original.SpaceLimit = 1024 * 1024 * 100 // 100MB
	original.AvailSpace = 1024 * 1024 * 75  // 75MB

	t.Run("Basic encode/decode", func(t *testing.T) {
		// 编码
		encoded := EncodeMMMeta(original)
		t.Logf("Encoded data length: %d bytes", len(encoded))

		// 解码
		decoded, err := DecodeMMMeta(encoded)
		if err != nil {
			t.Fatalf("Decode failed: %v", err)
		}

		// 验证字段
		if decoded.MemSpaceId != original.MemSpaceId {
			t.Errorf("MemSpaceId mismatch: got %d, want %d", decoded.MemSpaceId, original.MemSpaceId)
		}

		// ... 其他验证字段保持不变

		t.Logf("Test passed: all fields match")
		t.Logf("MemSpaceId: %d", decoded.MemSpaceId) // 新增日志
		t.Logf("BindingAgents: %v", decoded.BindingAgents)
		t.Logf("SpaceType: %v", decoded.SpaceType)
		t.Logf("SpaceStatus: %v", decoded.SpaceStatus)
		t.Logf("SpaceLimit: %d", decoded.SpaceLimit)
		t.Logf("AvailSpace: %d", decoded.AvailSpace)
	})

	// 其他测试用例也类似更新，确保包含 MemSpaceId
	t.Run("All space types and statuses", func(t *testing.T) {
		spaceTypes := []MemSpaceType{Private, Shared}
		spaceStatuses := []MemSpaceStatus{Pending, Binding, Corrupt, Writing}

		for _, spaceType := range spaceTypes {
			for _, spaceStatus := range spaceStatuses {
				testData := NewMemMetaData(1, Private, 1<<20)
				testData.MemSpaceId = uint64(spaceType)*10000 + uint64(spaceStatus) // 使用新字段
				testData.BindingAgents = []uint64{1, 2, 3}
				testData.SpaceType = spaceType
				testData.SpaceStatus = spaceStatus
				testData.SpaceLimit = 1000
				testData.AvailSpace = 500

				encoded := EncodeMMMeta(testData)
				decoded, err := DecodeMMMeta(encoded)
				if err != nil {
					t.Errorf("Failed for SpaceType=%v, SpaceStatus=%v: %v", spaceType, spaceStatus, err)
					continue
				}

				if decoded.MemSpaceId != testData.MemSpaceId {
					t.Errorf("MemSpaceId mismatch for %v: got %v, want %v", spaceType, decoded.MemSpaceId, testData.MemSpaceId)
				}
				// ... 其他验证
			}
		}
	})

	// 其他测试用例也类似更新...
}
