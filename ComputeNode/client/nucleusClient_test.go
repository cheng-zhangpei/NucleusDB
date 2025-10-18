package client

import (
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
)

func TestNucleusClientTxn(t *testing.T) {
	client := NewNucleusClient("127.0.0.1:31001", 1)
	results, err := client.Update(func(txn TxnOperation) error {
		// 在这里写你要执行的事务操作
		if err := txn.TxnPut([]byte("key10000"), []byte("value1")); err != nil {
			return err
		}
		if err := txn.TxnPut([]byte("key2"), []byte("value2")); err != nil {
			return err
		}
		if err := txn.TxnGet([]byte("key1")); err != nil {
			return err
		}
		if err := txn.TxnDelete([]byte("key2")); err != nil {
			return err
		}
		return nil
	})
	// 断言无错误
	assert.NoError(t, err)
	if len(results) == 0 {

	}
	// 打印结果
	for i, val := range results {
		t.Logf("Result[%d]: %s\n", i, val)
	}
}

func TestNucleusDBRaft(t *testing.T) {
	client := NewNucleusClient("127.0.0.1:31001", 1)
	testKey1 := []byte("testKey1")
	testValue1 := []byte("testValue1")
	err := client.DistributePut(testKey1, testValue1)
	assert.NoError(t, err)
	get, err := client.DistributeGet(testKey1)
	assert.NoError(t, err)
	//println("first")
	println(string(get))
	assert.Equal(t, testValue1, get)
	err = client.DistributeDelete(testKey1)
	assert.NoError(t, err)
	get, err = client.DistributeGet(testKey1)
	assert.NoError(t, err)
	log.Println(get)
}
