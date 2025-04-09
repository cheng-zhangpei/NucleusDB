package txn

import (
	"log"
	"testing"
	"time"
)

const test_Key1 = "test_Key"

func TestCoordinatorClient_HandleConflictCheck(t *testing.T) {
	client := NewCoordinatorClient(httpAddr)
	// 新创建一个虚拟的快照数据
	hashCode := GenerateKeyHashCode([]byte(test_Key1))
	// 根据Key生成一系列的operation

	snapshot := NewTxnSnapshot()
	test_op := &Operation{
		Cmd:   "GET",
		Key:   []byte(test_Key1),
		Value: []byte("testValue"),
	}
	// operation 并不会影响冲突检测结果
	//snapshot.PendingReads[hashCode] = test_op
	snapshot.PendingWrite[hashCode] = test_op
	snapshot.StartWatermark = GetCurrenTime()
	tempTime := snapshot.StartWatermark
	// 模拟事务提交在十秒之后
	snapshot.CommitTime = tempTime + uint64(time.Second.Microseconds()*5)
	err := client.SaveSnapshot(snapshot)
	if err != nil {
		panic(err)
	}
	// 这个位置要手动添加水位线
	log.Println("save finished!")
	checkList := make([]uint64, 0)
	// 将之前生成的冲突hashcode放入冲突检测列表
	checkList = append(checkList, hashCode)
	check, err := client.HandleConflictCheck(checkList, tempTime, tempTime+uint64(time.Second.Microseconds()*7))
	if err != nil {
		panic(err)
	}
	log.Println(check)
}
