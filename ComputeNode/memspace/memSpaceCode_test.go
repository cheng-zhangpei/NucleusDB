package memspace

// test the encode and decode method for memspace, its complexity is high be cautious
import (
	"testing"
)

func TestEncodeDecodeMMSpace(t *testing.T) {
	// 创建测试用的 MemSpace
	original := &MemSpace{
		MemSpaceId:    987654321,
		BindingAgents: []uint64{111, 222, 333, 444, 555},
		memUints: []*MemUint{
			{
				key:       []byte("test_key_1"),
				value:     []byte("test_value_1"),
				unitType:  ComputeType(1), // 假设 ComputeType 是某种整数类型
				timestamp: 1640995200000,  // 2022-01-01
			},
			{
				key:       []byte("key_2"),
				value:     []byte("value_2"),
				unitType:  ComputeType(2),
				timestamp: 1641081600000, // 2022-01-02
			},
		},
		TempMemUnits: []*TempMemUnit{ // 这个字段不应该被编码
			{
				Value:     "temp_value",
				Timestamp: 1641168000000,
			},
		},
		vectorUints:         []*VectorRecord{}, // 这个字段不应该被编码
		spaceType:           Shared,
		spaceStatus:         Binding,
		spaceLimit:          1024 * 1024 * 100, // 100MB
		availSpace:          1024 * 1024 * 75,  // 75MB
		description:         "Test memory space for unit testing",
		name:                "TestSpace",
		memSpaceContentType: ContentMemory,
	}

	t.Run("Basic encode/decode", func(t *testing.T) {
		// 编码
		encoded, err := EncodeMMSpace(original)
		if err != nil {
			t.Fatalf("Encode failed: %v", err)
		}
		t.Logf("Encoded data length: %d bytes", len(encoded))

		// 解码
		decoded, err := DecodeMMSpace(encoded)
		if err != nil {
			t.Fatalf("Decode failed: %v", err)
		}

		if decoded.MemSpaceId != original.MemSpaceId {
			t.Errorf("MemSpaceId mismatch: got %d, want %d", decoded.MemSpaceId, original.MemSpaceId)
		}

		// 验证 bindingAgents
		if len(decoded.BindingAgents) != len(original.BindingAgents) {
			t.Errorf("BindingAgents length mismatch: got %d, want %d", len(decoded.BindingAgents), len(original.BindingAgents))
		} else {
			for i, agentId := range original.BindingAgents {
				if decoded.BindingAgents[i] != agentId {
					t.Errorf("BindingAgents[%d] mismatch: got %d, want %d", i, decoded.BindingAgents[i], agentId)
				}
			}
		}

		// 验证 memUints
		if len(decoded.memUints) != len(original.memUints) {
			t.Errorf("MemUints length mismatch: got %d, want %d", len(decoded.memUints), len(original.memUints))
		} else {
			for i, memUnit := range original.memUints {
				decodedUnit := decoded.memUints[i]
				if string(decodedUnit.key) != string(memUnit.key) {
					t.Errorf("MemUint[%d] key mismatch: got %s, want %s", i, string(decodedUnit.key), string(memUnit.key))
				}
				if string(decodedUnit.value) != string(memUnit.value) {
					t.Errorf("MemUint[%d] value mismatch: got %s, want %s", i, string(decodedUnit.value), string(memUnit.value))
				}
				if decodedUnit.unitType != memUnit.unitType {
					t.Errorf("MemUint[%d] unitType mismatch: got %d, want %d", i, decodedUnit.unitType, memUnit.unitType)
				}
				if decodedUnit.timestamp != memUnit.timestamp {
					t.Errorf("MemUint[%d] timestamp mismatch: got %d, want %d", i, decodedUnit.timestamp, memUnit.timestamp)
				}
			}
		}

		// 验证其他字段
		if decoded.spaceType != original.spaceType {
			t.Errorf("SpaceType mismatch: got %v, want %v", decoded.spaceType, original.spaceType)
		}

		if decoded.spaceStatus != original.spaceStatus {
			t.Errorf("SpaceStatus mismatch: got %v, want %v", decoded.spaceStatus, original.spaceStatus)
		}

		if decoded.spaceLimit != original.spaceLimit {
			t.Errorf("SpaceLimit mismatch: got %d, want %d", decoded.spaceLimit, original.spaceLimit)
		}

		if decoded.availSpace != original.availSpace {
			t.Errorf("AvailSpace mismatch: got %d, want %d", decoded.availSpace, original.availSpace)
		}

		if decoded.description != original.description {
			t.Errorf("Description mismatch: got %s, want %s", decoded.description, original.description)
		}

		if decoded.name != original.name {
			t.Errorf("Name mismatch: got %s, want %s", decoded.name, original.name)
		}

		if decoded.memSpaceContentType != original.memSpaceContentType {
			t.Errorf("MemSpaceContentType mismatch: got %v, want %v", decoded.memSpaceContentType, original.memSpaceContentType)
		}

		// 验证 TempMemUnits 和 vectorUints 没有被编码（应该为空）
		if len(decoded.TempMemUnits) != 0 {
			t.Errorf("TempMemUnits should be empty after decode, but got %d elements", len(decoded.TempMemUnits))
		}

		if len(decoded.vectorUints) != 0 {
			t.Errorf("VectorUints should be empty after decode, but got %d elements", len(decoded.vectorUints))
		}

		t.Logf("Test passed: all fields match")
		t.Logf("MemSpaceId: %d", decoded.MemSpaceId)
		t.Logf("BindingAgents: %v", decoded.BindingAgents)
		t.Logf("MemUints count: %d", len(decoded.memUints))
		t.Logf("SpaceType: %v", decoded.spaceType)
		t.Logf("SpaceStatus: %v", decoded.spaceStatus)
		t.Logf("SpaceLimit: %d", decoded.spaceLimit)
		t.Logf("AvailSpace: %d", decoded.availSpace)
		t.Logf("Description: %s", decoded.description)
		t.Logf("Name: %s", decoded.name)
		t.Logf("MemSpaceContentType: %v", decoded.memSpaceContentType)
	})

	t.Run("Empty MemSpace", func(t *testing.T) {
		emptySpace := &MemSpace{
			MemSpaceId:          0,
			BindingAgents:       []uint64{},
			memUints:            []*MemUint{},
			TempMemUnits:        []*TempMemUnit{},
			vectorUints:         []*VectorRecord{},
			spaceType:           Private,
			spaceStatus:         Pending,
			spaceLimit:          0,
			availSpace:          0,
			description:         "",
			name:                "",
			memSpaceContentType: ToolMemory,
		}

		encoded, err := EncodeMMSpace(emptySpace)
		if err != nil {
			t.Fatalf("Encode empty space failed: %v", err)
		}

		decoded, err := DecodeMMSpace(encoded)
		if err != nil {
			t.Fatalf("Decode empty space failed: %v", err)
		}

		// 验证空值
		if decoded.MemSpaceId != 0 ||
			len(decoded.BindingAgents) != 0 || len(decoded.memUints) != 0 ||
			decoded.spaceType != Private || decoded.spaceStatus != Pending ||
			decoded.spaceLimit != 0 || decoded.availSpace != 0 ||
			decoded.description != "" || decoded.name != "" ||
			decoded.memSpaceContentType != ToolMemory {
			t.Errorf("Empty space decode mismatch")
		}

		t.Logf("Empty space test passed")
	})

	t.Run("All space types and statuses", func(t *testing.T) {
		spaceTypes := []MemSpaceType{Private, Shared}
		spaceStatuses := []MemSpaceStatus{Pending, Binding, Corrupt, Writing}
		contentTypes := []MemSpaceContentType{ToolMemory, ContentMemory, BehavioralMemory, EpisodicMemory}

		for _, spaceType := range spaceTypes {
			for _, spaceStatus := range spaceStatuses {
				for _, contentType := range contentTypes {
					testData := &MemSpace{
						MemSpaceId:          uint64(spaceType)*10000 + uint64(spaceStatus),
						BindingAgents:       []uint64{1, 2, 3},
						memUints:            []*MemUint{},
						TempMemUnits:        []*TempMemUnit{},
						vectorUints:         []*VectorRecord{},
						spaceType:           spaceType,
						spaceStatus:         spaceStatus,
						spaceLimit:          1000,
						availSpace:          500,
						description:         "Test",
						name:                "Test",
						memSpaceContentType: contentType,
					}

					encoded, err := EncodeMMSpace(testData)
					if err != nil {
						t.Errorf("Encode failed for SpaceType=%v, SpaceStatus=%v, ContentType=%v: %v",
							spaceType, spaceStatus, contentType, err)
						continue
					}

					decoded, err := DecodeMMSpace(encoded)
					if err != nil {
						t.Errorf("Decode failed for SpaceType=%v, SpaceStatus=%v, ContentType=%v: %v",
							spaceType, spaceStatus, contentType, err)
						continue
					}

					if decoded.spaceType != spaceType {
						t.Errorf("SpaceType mismatch for %v: got %v, want %v",
							spaceType, decoded.spaceType, spaceType)
					}

					if decoded.spaceStatus != spaceStatus {
						t.Errorf("SpaceStatus mismatch for %v: got %v, want %v",
							spaceStatus, decoded.spaceStatus, spaceStatus)
					}

					if decoded.memSpaceContentType != contentType {
						t.Errorf("ContentType mismatch for %v: got %v, want %v",
							contentType, decoded.memSpaceContentType, contentType)
					}
				}
			}
		}
		t.Logf("All combinations test completed")
	})

	t.Run("Large data", func(t *testing.T) {
		largeSpace := &MemSpace{
			MemSpaceId:          888,
			BindingAgents:       make([]uint64, 1000),
			memUints:            make([]*MemUint, 100),
			spaceType:           Shared,
			spaceStatus:         Binding,
			spaceLimit:          1 << 30, // 1GB
			availSpace:          1 << 29, // 512MB
			description:         "This is a very long description for testing large data encoding and decoding performance and correctness",
			name:                "LargeTestSpace",
			memSpaceContentType: BehavioralMemory,
		}

		// 填充 bindingAgents
		for i := range largeSpace.BindingAgents {
			largeSpace.BindingAgents[i] = uint64(i)
		}

		// 填充 memUints
		for i := range largeSpace.memUints {
			largeSpace.memUints[i] = &MemUint{
				key:       []byte("large_key_" + string(rune(i))),
				value:     make([]byte, 1024), // 1KB value
				unitType:  ComputeType(i % 10),
				timestamp: 1640995200000 + uint64(i)*1000,
			}
			// 填充一些测试数据
			for j := range largeSpace.memUints[i].value {
				largeSpace.memUints[i].value[j] = byte((i + j) % 256)
			}
		}

		encoded, err := EncodeMMSpace(largeSpace)
		if err != nil {
			t.Fatalf("Encode large space failed: %v", err)
		}
		t.Logf("Large space encoded: %d bytes", len(encoded))

		decoded, err := DecodeMMSpace(encoded)
		if err != nil {
			t.Fatalf("Decode large space failed: %v", err)
		}

		// 基本验证
		if len(decoded.BindingAgents) != len(largeSpace.BindingAgents) {
			t.Errorf("Large space bindingAgents length mismatch: got %d, want %d",
				len(decoded.BindingAgents), len(largeSpace.BindingAgents))
		}

		if len(decoded.memUints) != len(largeSpace.memUints) {
			t.Errorf("Large space memUints length mismatch: got %d, want %d",
				len(decoded.memUints), len(largeSpace.memUints))
		}

		t.Logf("Large data test passed")
	})
}
