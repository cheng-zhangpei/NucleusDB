package NucleusDB

import (
	"NucleusDB/data"
	"encoding/binary"
	"sync"
	"sync/atomic"
)

var txnFinKey = []byte("txn-fin")

const nonTransactionSeqNo = 0

// WriteBatch 原子写保证事务一致性
type WriteBatch struct {
	options WriteBatchOptions
	mu      *sync.Mutex
	db      *DB
	// 暂存用户写入的LogRecord,这部分的数据是被放在内存中的
	pendingWrite map[string]*data.LogRecord
}

// NewWriteBatch  初始化
func (db *DB) NewWriteBatch(options WriteBatchOptions) *WriteBatch {
	// 需要加上b+树索引的限制
	// 如果序列文件不存在并且不是第一次启动的时候序列文件不存在
	// 这里稍微注意一下使用的条件：
	// 1. 索引不是B+Tree
	// 2. 不是第一次加载（第一次加载的时候不会生成事务序列号，无法初始化db的任务序列号）
	// 3. 不是首次加载，因为序列号文件是第一次数据库实例关闭之后才会生成
	// 综上，如果不是首次加载，是可以使用B+树索引
	if db.options.IndexerType == BPTree && !db.seqNoExist && !db.isInitial {
		panic("cannot use write batch ,seq nofile exist")
	}
	return &WriteBatch{
		options:      options,
		mu:           new(sync.Mutex),
		db:           db,
		pendingWrite: make(map[string]*data.LogRecord),
	}
}

func (wb *WriteBatch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	// 加锁
	wb.mu.Lock()
	defer wb.mu.Unlock()
	// 暂存LogRecord
	logRecord := data.LogRecord{Key: key, Value: value}
	wb.pendingWrite[string(key)] = &logRecord
	return nil
}

func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()
	// 数据不存在就直接返回
	logRecordPos := wb.db.index.Get(key)
	if logRecordPos == nil {
		// 如果暂存区有数据就直接删除
		if wb.pendingWrite[string(key)] != nil {
			delete(wb.pendingWrite, string(key))
		}
		return nil
	}
	// 也是将LogRecord暂存起来
	logRecord := data.LogRecord{Key: key, Type: data.LogRecordDeleted}
	wb.pendingWrite[string(key)] = &logRecord
	return nil
}

// Commit 提交事务，将批量数据全部写到磁盘并且更新索引，此处更新索引一定是放在持久化之后的
func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	// 如果暂存区中没有数据就无法进行Commit
	if len(wb.pendingWrite) == 0 {
		return nil
	}
	// 如果暂存区中的数据超过了最大数据量
	if uint32(len(wb.pendingWrite)) > wb.options.MaxBatchNum {
		return ErrExceedMaxBatchNum
	}
	// 开始实际去写入数据
	// 获取当前事务的序列号,需要原子写入
	wb.db.mu.Lock()
	defer wb.db.mu.Unlock()
	// 保证事务提交的串行化,所以事务序列号是由DB来进行维护的
	positions := make(map[string]*data.LogRecordPos) // 暂存数据，以此来批量更新内存索引
	seqNo := atomic.AddUint64(&wb.db.seqNo, 1)
	for _, record := range wb.pendingWrite {
		// 将附带有事务序列的Key写入活跃文件
		logRecordPos, err := wb.db.appendLogRecord(&data.LogRecord{
			// 在持久化的过程中是需要给key加入序列号的
			Key:   logRecordKeyWithSeq(record.Key, seqNo),
			Value: record.Value,
			Type:  record.Type,
		})
		if err != nil {
			return err
		}
		positions[string(record.Key)] = logRecordPos
	}
	// 加上标识事务完成的数据 ->  标志着这个序列号的事务完成
	finishedRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(txnFinKey, wb.db.seqNo),
		Type: data.LogRecordTxnFinished,
	}
	if _, err := wb.db.appendLogRecord(finishedRecord); err != nil {
		return err
	}
	if wb.options.SyncWrite && wb.db.activeFile != nil {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}
	// 更新内存索引
	for _, record := range wb.pendingWrite {
		pos := positions[string(record.Key)]
		var oldPos *data.LogRecordPos
		if record.Type == data.LogRecordNormal {
			oldPos = wb.db.index.Put(record.Key, pos)
		}
		if record.Type == data.LogRecordDeleted {
			oldPos, _ = wb.db.index.Delete(record.Key)
		}
		if oldPos != nil {
			wb.db.reclaimSize += oldPos.Size
		}
	}
	// 清空暂存数据
	wb.pendingWrite = make(map[string]*data.LogRecord)
	return nil
}

// key+Seq Number 编码
func logRecordKeyWithSeq(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqNo)

	encKey := make([]byte, n+len(key))
	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)

	return encKey
}

// 解析 LogRecord 的 key，获取实际的 key 和事务序列号
func parseLogRecordKey(key []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(key)
	realKey := key[n:]
	return realKey, seqNo
}
