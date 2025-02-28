package raft

import (
	"ComDB"
	"ComDB/raft/pb"
	"ComDB/search"
	"fmt"
	"log"
)

// this is the application that communicate with node
// here is another go routine
const LogEntriesPrefix = "raft-log-sync"

type applyEntry struct {
	Command string
	Key     string
	Value   string
}
type application struct {
	ID uint64
	DB *ComDB.DB
	// 将需要应用的内容发送给上层的通道
	applyc chan []*applyEntry
	// 日志持久化的通道
	commitc chan []*pb.Entry
}

func newApplication(option ComDB.Options, raftNodeId uint64) (*application, error) {
	db, err := ComDB.Open(option)
	if err != nil {
		return nil, err
	}
	application := &application{
		ID:      raftNodeId,
		DB:      db,
		applyc:  make(chan []*applyEntry, 10),
		commitc: make(chan []*pb.Entry, 10),
	}
	// 开启一个守护线程监控通道是否有信息
	// 这里只开一个守护线程主要是想尽量保证apply在commit之后
	application.Listener()
	return application, nil
}

// commit 提交并 持久化日志
func (app *application) commit(ents []*pb.Entry) error {
	logLen := app.getPrefix(LogEntriesPrefix)
	// 持久化的日志是顺序记录的
	for _, ent := range ents {
		data := []byte(ent.Data)
		key := []byte(fmt.Sprintf("%s-%d-%d", LogEntriesPrefix, app.ID, logLen))
		err := app.DB.Put(key, data)
		if err != nil {
			return err
		}
	}
	return nil
}

// apply 应用状态到状态机
func (app *application) applyAll(appEnts []*applyEntry) error {
	for _, appEnt := range appEnts {
		_, err := app.apply(appEnt.Command, appEnt.Key, appEnt.Value)
		if err != nil {
			return err
		}
	}
	return nil
}
func (app *application) apply(command string, key string, value string) (string, error) {
	opts := ComDB.DefaultCompressOptions

	switch command {
	case "PUT":
		// do put -> 直接将数据放到本地的数据库里面就ok了
		err := app.DB.Put([]byte(key), []byte(value))
		if err != nil {
			return "", err
		}
	case "DELETE":
		// do delete -> 删除本地数据库里面的数据
		err := app.DB.Delete([]byte(key))
		if err != nil {
			return "", err
		}
	case "CreateMemoryMeta":
		// key 为agentID，value为totalSize（记忆空间大小）
		meta := search.NewMemoryMeta(key, 20)
		enMeta := meta.Encode()
		// 现在是一个空的记忆空间
		_ = app.DB.Put([]byte(key), enMeta)
	case "Compress":
		ms := &search.MemoryStructure{
			Db: app.DB,
		}
		// 获取meta并装载=> 此时记忆空间装载完成
		metaData, err := ms.FindMetaData([]byte(key))
		if err != nil {
			return "", err
		}
		ms.Mm = metaData
		compressor, err := search.NewCompressor(opts, key, ms) // 假设 NewCompressor 是 Compressor 的构造函数
		if err != nil {
			return "", err
		}
		_, err = compressor.Compress(key, value)
		if err != nil {
			return "", err
		}
	case "MemSet":
		// 此处的key是agentId，value是放入记忆空间的值
		ms := &search.MemoryStructure{
			Db: app.DB,
		}

		// 调用 MMSet 方法
		if err := ms.MMSet([]byte(value), key); err != nil {
			return "", err
		}
		log.Printf("mem has been insert into the memorySpace of agent:%s!\n", key)
	case "MemDel":
		ms := &search.MemoryStructure{
			Db: app.DB,
		}

		// 调用 MMDel 方法
		if err, _ := ms.MMDel(key); err != true {
			log.Fatalln("memory delete fault!")
			return "", nil
		}
	}
	return "", nil
}

// Listener 在Application端收到通道信号的操作
func (app *application) Listener() {
	go func() {
		for {
			select {
			case c := <-app.commitc:
				err := app.commit(c)
				if err != nil {
					log.Printf("Error committed entries: %v\n", err)
					panic(err)
				}
			case a := <-app.applyc:
				err := app.applyAll(a)
				if err != nil {
					// 处理错误，例如记录日志
					log.Printf("Error applying entries: %v\n", err)
					panic(err)
				}
			}
		}
	}()
}

// getPrefix 用之前写的迭代器来实现前缀查找
func (app *application) getPrefix(prefix string) uint64 {
	iterator := app.DB.NewIterator(ComDB.IteratorOptions{
		Prefix:  []byte(prefix),
		Reverse: false, // 是否反向遍历
	})
	defer iterator.Close()
	// 存储匹配的键值对
	prefixLen := 0
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		key := iterator.Key()
		_, err := app.DB.Get(key)
		if err != nil {
			continue
		}
		prefixLen++
	}
	return uint64(prefixLen)
}
