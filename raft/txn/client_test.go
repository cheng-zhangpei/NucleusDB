package txn

import (
	"log"
	"testing"
)

func TestCoordinatorClient_HandleConflictCheck(t *testing.T) {
	client := NewCoordinatorClient(httpAddr)
	// 新创建一个虚拟的快照数据
	snapshot := NewTxnSnapshot()
	snapshot.StartWatermark = GetCurrenTime()
	snapshot.CommitTime = GetCurrenTime() + 10000
	err := client.SaveSnapshot(snapshot)
	if err != nil {
		panic(err)
	}
	log.Println("save finished!")
	checkList := make([]uint64, 0)
	checkList = append(checkList, 1)
	_, err = client.HandleConflictCheck(checkList, GetCurrenTime(), GetCurrenTime()+100000)
	if err != nil {
		panic(err)
	}

}
