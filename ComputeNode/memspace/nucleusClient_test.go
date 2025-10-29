package memspace

import (
	"github.com/stretchr/testify/assert"
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

func TestNucleusDBRaftPut(t *testing.T) {
	client := NewNucleusClient("127.0.0.1:31001", 1)
	testKey1 := []byte("testKey3")
	testValue1 := []byte("testValue2")

	testKey2 := []byte("testKey4")
	testValue2 := []byte("testValue4")

	testKey3 := []byte("testKey3")
	testValue3 := []byte("testValue3")

	err := client.DistributePut(testKey1, testValue1)
	assert.NoError(t, err)
	err = client.DistributePut(testKey2, testValue2)
	assert.NoError(t, err)
	err = client.DistributePut(testKey3, testValue3)
	assert.NoError(t, err)

	//get, err := dbClient.DistributeGet(testKey1)
	//assert.NoError(t, err)
	////println("first")
	//println(string(get))
	//assert.Equal(t, testValue1, get)
	//err = dbClient.DistributeDelete(testKey1)
	//assert.NoError(t, err)
	//get, err := dbClient.DistributeGet(testKey1)
	//assert.NoError(t, err)
	//log.Println(get)
}

func TestRaftGet(t *testing.T) {
	client := NewNucleusClient("127.0.0.1:31002", 2)

	testKey1 := []byte("testKey3")
	//testValue1 := []byte("testValue2")

	testKey2 := []byte("testKey4")
	//testValue2 := []byte("testValue4")

	testKey3 := []byte("testKey3")
	//testValue3 := []byte("testValue3")

	get1, err := client.DistributeGet(testKey1)
	assert.NoError(t, err)
	println(string(get1))
	get2, err := client.DistributeGet(testKey2)
	assert.NoError(t, err)
	println(string(get2))
	get3, err := client.DistributeGet(testKey3)
	assert.NoError(t, err)
	println(string(get3))

}

func TestNucleusDBRaftDelete(t *testing.T) {
	client := NewNucleusClient("127.0.0.1:31001", 1)
	testKey1 := []byte("testKey3")

	testKey2 := []byte("testKey4")

	testKey3 := []byte("testKey3")

	err := client.DistributeDelete(testKey1)
	assert.NoError(t, err)
	err = client.DistributeDelete(testKey2)
	assert.NoError(t, err)
	err = client.DistributeDelete(testKey3)
	assert.NoError(t, err)

	//get, err := dbClient.DistributeGet(testKey1)
	//assert.NoError(t, err)
	////println("first")
	//println(string(get))
	//assert.Equal(t, testValue1, get)
	//err = dbClient.DistributeDelete(testKey1)
	//assert.NoError(t, err)
	//get, err := dbClient.DistributeGet(testKey1)
	//assert.NoError(t, err)
	//log.Println(get)
}
