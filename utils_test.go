package NucleusDB

import (
	"reflect"
	"testing"
)

func TestEncodeDecodeTxn(t *testing.T) {
	tests := []struct {
		name string
		txn  *Txn
	}{
		{
			name: "full_features",
			txn:  createFullTxn(),
		},
		{
			name: "empty_maps",
			txn: &Txn{
				pendingWrite:        make(map[uint64]*operation),
				pendingRepeatWrites: make(map[uint64]*operation),
				pendingReads:        make(map[uint64]*operation),
				conflictKeys:        make(map[uint64]struct{}),
				operations:          []*operation{},
				getResult:           []string{},
			},
		},
		{
			name: "multiple_operations",
			txn: &Txn{
				pendingWrite:        make(map[uint64]*operation),
				pendingRepeatWrites: make(map[uint64]*operation),
				pendingReads:        make(map[uint64]*operation),
				conflictKeys:        make(map[uint64]struct{}),
				operations: []*operation{
					{cmd: "PUT", key: []byte("key1"), value: []byte("value1")},
					{cmd: "GET", key: []byte("key2")},
					{cmd: "DELETE", key: []byte("key3")},
				},
				getResult: []string{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 执行编码
			encoded := EncodeTxn(tt.txn)

			// 执行解码
			decoded := DecodeTxn(encoded)

			// 忽略DB字段的比较
			decoded.db = tt.txn.db

			// 深度比较结构体
			if !reflect.DeepEqual(tt.txn, decoded) {
				t.Errorf("Decoded txn mismatch\nOriginal: %+v\nDecoded:  %+v", tt.txn, decoded)
			}

			// 特别检查非PUT操作的value字段
			for i, op := range tt.txn.operations {
				decodedOp := decoded.operations[i]
				if op.cmd != "PUT" && decodedOp.value != nil {
					t.Errorf("Operation %d should have nil value, got: %v", i, decodedOp.value)
				}
			}
		})
	}
}

// 创建包含所有字段的测试用例
func createFullTxn() *Txn {
	return &Txn{
		startWatermark: 123456789,
		commitTime:     987654321,
		pendingWrite: map[uint64]*operation{
			0x1234: {cmd: "PUT", key: []byte("key1"), value: []byte("value1")},
			0x5678: {cmd: "DELETE", key: []byte("key2")},
		},
		pendingRepeatWrites: map[uint64]*operation{
			0x9ABC: {cmd: "PUT", key: []byte("key3"), value: []byte("value3")},
		},
		pendingReads: map[uint64]*operation{
			0xDEF0: {cmd: "GET", key: []byte("key4")},
		},
		conflictKeys: map[uint64]struct{}{
			0x1234: {},
			0x5678: {},
		},
		operations: []*operation{
			{cmd: "PUT", key: []byte("op1"), value: []byte("val1")},
			{cmd: "GET", key: []byte("op2")},
		},
		getResult: []string{"result1", "result2"},
	}
}

func TestEdgeCases(t *testing.T) {
	t.Run("large_values", func(t *testing.T) {
		bigData := make([]byte, 1<<20) // 1MB数据
		txn := &Txn{
			pendingWrite: map[uint64]*operation{
				1: {cmd: "PUT", key: []byte("big_key"), value: bigData},
			},
		}

		encoded := EncodeTxn(txn)
		decoded := DecodeTxn(encoded)

		if !reflect.DeepEqual(txn.pendingWrite[1].value, decoded.pendingWrite[1].value) {
			t.Error("Big value mismatch")
		}
	})

	t.Run("special_characters", func(t *testing.T) {
		specialStrings := []string{"", "null\x00", "中文", "🎉"}
		txn := &Txn{getResult: specialStrings}

		encoded := EncodeTxn(txn)
		decoded := DecodeTxn(encoded)

		if !reflect.DeepEqual(txn.getResult, decoded.getResult) {
			t.Errorf("Special strings mismatch\nGot: %q\nWant: %q", decoded.getResult, specialStrings)
		}
	})
}

func TestConsistency(t *testing.T) {
	original := createFullTxn()

	// 多次编码解码验证稳定性
	for i := 0; i < 3; i++ {
		encoded := EncodeTxn(original)
		decoded := DecodeTxn(encoded)
		decoded.db = original.db

		if !reflect.DeepEqual(original, decoded) {
			t.Fatalf("Consistency check failed at iteration %d", i)
		}
		original = decoded // 使用解码结果继续测试
	}
}
