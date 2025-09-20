package redis

import (
	"NucleusDB"
	"NucleusDB/utils"
	"encoding/binary"
	"errors"
	"time"
)

type RedisDataType = byte

var (
	ErrWrongTypeOperation = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
)

const (
	String RedisDataType = iota
	Hash
	Set
	List
	ZSet
	BitMap
)

// redis 结构体
type RedisDataStructure struct {
	db *NucleusDB.DB
}

func NewRedisDataStructure(options NucleusDB.Options) (*RedisDataStructure, error) {
	db, err := NucleusDB.Open(options)
	if err != nil {
		return nil, err
	}
	return &RedisDataStructure{
		db: db,
	}, nil
}

// ===================================String 数据结构===================================================

func (rds *RedisDataStructure) Set(key []byte, ttl time.Duration, value []byte) error {
	if value == nil {
		return nil
	}
	// 编码 key -> (type + expired + payload)
	buf := make([]byte, binary.MaxVarintLen64+1)
	buf[0] = String
	var index = 1
	var expire int64 = 0
	if ttl != 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}
	index += binary.PutVarint(buf[index:], expire)
	encValue := make([]byte, index+len(value))
	copy(encValue[:index], buf[:index])
	copy(encValue[index:], value)
	return rds.db.Put(key, encValue)
}

func (rds *RedisDataStructure) Get(key []byte) ([]byte, error) {
	encValue, err := rds.db.Get(key)
	if err != nil {
		return nil, err
	}
	// 对编码数据进行解码
	var index = 1
	dataType := encValue[0]
	if dataType != String {
		return nil, ErrWrongTypeOperation
	}
	expire, n := binary.Varint(encValue[index:])
	index += n
	// 判断是否过期
	if expire > 0 && expire <= time.Now().UnixNano() {
		return nil, nil
	}
	// index后面的数据就是value了
	return encValue[index:], nil
}

// ===================================Hash 数据结构===================================================
// key ---> hashmap --->field标识不同的value，所以严格来说field才是"key"
func (rds *RedisDataStructure) HSet(key, field, value []byte) (bool, error) {
	// 先查找元数据
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}
	// 构造数据部分的key-->其实很好理解就是在这种情况下key和value都是一个复合体有多个不同的字段共同控制
	hk := &hashInternalKey{
		key:     key,
		version: meta.version, // 用version来进行不同HashSet之间的区分
		field:   field,
	}
	// 先判断数据是否存在
	encKey := hk.encode()
	var exist = true
	if _, err := rds.db.Get(encKey); err == NucleusDB.ErrKeyNotFound {
		exist = false
	}
	// 新建一个事务
	wb := rds.db.NewWriteBatch(NucleusDB.DefaultWriteBatchOptions)
	// 这个事务需要完成两件事情一个是保存元数据，并根据加密的key存储value值
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	}
	_ = wb.Put(encKey, value)
	if err := wb.Commit(); err != nil {
		return false, err
	}
	return !exist, nil
	// todo 其实写完看这段代码还是感觉事务安排不是非常好效率很一般

}

func (rds *RedisDataStructure) HGet(key, field []byte) ([]byte, error) {
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}

	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}

	return rds.db.Get(hk.encode())
}

func (rds *RedisDataStructure) HDel(key, field []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}
	encKey := hk.encode()

	// 先查看是否存在
	var exist = true
	if _, err = rds.db.Get(encKey); err == NucleusDB.ErrKeyNotFound {
		exist = false
	}

	if exist {
		wb := rds.db.NewWriteBatch(NucleusDB.DefaultWriteBatchOptions)
		meta.size--
		_ = wb.Put(key, meta.encode())
		_ = wb.Delete(encKey)
		if err = wb.Commit(); err != nil {
			return false, err
		}
	}

	return exist, nil
}

func (rds *RedisDataStructure) findMetadata(key []byte, dataType RedisDataType) (*metadata, error) {
	metaData, err := rds.db.Get(key)
	if err != nil && !errors.Is(err, NucleusDB.ErrKeyNotFound) {
		return nil, err
	}
	var meta *metadata
	var exists = true
	if err == NucleusDB.ErrKeyNotFound {
		exists = false
	} else {
		meta = decodeMetaData(metaData)
		// 判断数据类型
		if meta.dataType != dataType {
			return nil, ErrWrongTypeOperation
		}
		// 判断过期时间
		if meta.expire != 0 && meta.expire < time.Now().UnixNano() {
			exists = false
		}
	}
	if !exists {
		meta = &metadata{
			dataType: dataType,
			expire:   0,
			version:  time.Now().UnixNano(),
			size:     0,
		}
		if dataType == List {
			meta.head = initialListMark
			meta.tail = initialListMark
		}
	}
	return meta, nil
}

func (rds *RedisDataStructure) Close() error {
	return rds.db.Close()
}

// ===================================List 数据结构===================================================
func (rds *RedisDataStructure) LPush(key []byte, element []byte) (uint32, error) {

	return rds.pushInner(key, element, true)

}
func (rds *RedisDataStructure) RPush(key []byte, element []byte) (uint32, error) {

	return rds.pushInner(key, element, false)

}
func (rds *RedisDataStructure) pushInner(key []byte, element []byte, isLeft bool) (uint32, error) {
	// 查找元数据
	meta, err := rds.findMetadata(key, List)
	if err != nil {
		return 0, err
	}
	// Hash 和 List 都有一个所谓的内部 key
	// 这个key会记录一下未当前的index是啥
	lk := &ListInternalKey{
		key:     key,
		version: meta.version,
	}
	//  根据头尾设置index
	if isLeft {
		lk.index = meta.head - 1
	} else {
		lk.index = meta.tail
	}
	wb := rds.db.NewWriteBatch(NucleusDB.DefaultWriteBatchOptions)
	meta.size++
	if isLeft {
		// 前移head
		meta.head--
	} else {
		// 后移tail
		meta.tail++
	}
	_ = wb.Put(key, meta.encode())
	_ = wb.Put(lk.encode(), element)
	// 提交事务
	if err := wb.Commit(); err != nil {
		return 0, err
	}
	return meta.size, nil
}

func (rds *RedisDataStructure) popInner(key []byte, isLeft bool) ([]byte, error) {
	// 查找元数据
	meta, err := rds.findMetadata(key, List)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}
	lk := &ListInternalKey{
		key:     key,
		version: meta.version,
	}
	//  根据头尾设置index
	if isLeft {
		lk.index = meta.head
	} else {
		lk.index = meta.tail - 1
	}

	element, err := rds.db.Get(lk.encode())
	if err != nil {
		return nil, err
	}
	//更新元数据
	meta.size--
	if isLeft {
		meta.head++
	} else {
		meta.tail--
	}
	// 保存元数据
	if err = rds.db.Put(key, meta.encode()); err != nil {
		return nil, err
	}
	return element, nil
}

func (rds *RedisDataStructure) LPop(key []byte) ([]byte, error) {
	return rds.popInner(key, true)
}

func (rds *RedisDataStructure) RPop(key []byte) ([]byte, error) {
	return rds.popInner(key, false)
}

//==================================Set ==========================================

func (rds *RedisDataStructure) SAdd(key []byte, member []byte) (bool, error) {
	// 查找元数据
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, nil
	}

	// 构造key
	sk := &SetInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}
	var ok bool
	if _, err := rds.db.Get(sk.encode()); errors.Is(err, NucleusDB.ErrKeyNotFound) {
		// 不存在,也就是说只有member在同一个meta中不同的时候才需要进行更新
		wb := rds.db.NewWriteBatch(NucleusDB.DefaultWriteBatchOptions)
		meta.size++
		_ = wb.Put(key, meta.encode())
		_ = wb.Put(sk.encode(), member)
		if err := wb.Commit(); err != nil {
			return false, err
		}
		ok = true
	}
	return ok, nil
}

func (rds *RedisDataStructure) SIsMember(key, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, nil
	}
	sk := &SetInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}
	// 去查找
	_, err = rds.db.Get(sk.encode())
	if err != nil && !errors.Is(err, NucleusDB.ErrKeyNotFound) {
		return false, nil
	}
	if errors.Is(err, NucleusDB.ErrKeyNotFound) {
		return false, nil
	}
	return true, nil
}

func (rds *RedisDataStructure) SRem(key, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	// 构造一个数据部分的 key
	sk := &SetInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	if _, err = rds.db.Get(sk.encode()); errors.Is(err, NucleusDB.ErrKeyNotFound) {
		return false, nil
	}

	// 更新
	wb := rds.db.NewWriteBatch(NucleusDB.DefaultWriteBatchOptions)
	meta.size--
	// 设置一个事务也就是先Put保存元数据，保存元数据之后再删除数据部分的内容

	_ = wb.Put(key, meta.encode())
	_ = wb.Delete(sk.encode())
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// ====================ZSET==========================

// ======================= ZSet 数据结构 =======================

func (rds *RedisDataStructure) ZAdd(key []byte, score float64, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, ZSet)
	if err != nil {
		return false, err
	}

	// 构造数据部分的key
	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		score:   score,
		member:  member,
	}

	var exist = true
	// 查看是否已经存在
	value, err := rds.db.Get(zk.encodeWithMember())
	if err != nil && err != NucleusDB.ErrKeyNotFound {
		return false, err
	}
	if err == NucleusDB.ErrKeyNotFound {
		exist = false
	}
	if exist {
		if score == utils.FloatFromBytes(value) {
			return false, nil
		}
	}

	// 更新元数据和数据
	wb := rds.db.NewWriteBatch(NucleusDB.DefaultWriteBatchOptions)
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	}
	if exist {
		oldKey := &zsetInternalKey{
			key:     key,
			version: meta.version,
			member:  member,
			score:   utils.FloatFromBytes(value),
		}
		_ = wb.Delete(oldKey.encodeWithScore())
	}
	_ = wb.Put(zk.encodeWithMember(), utils.Float64ToBytes(score))
	_ = wb.Put(zk.encodeWithScore(), nil)
	if err = wb.Commit(); err != nil {
		return false, err
	}

	return !exist, nil
}

func (rds *RedisDataStructure) ZScore(key []byte, member []byte) (float64, error) {
	meta, err := rds.findMetadata(key, ZSet)
	if err != nil {
		return -1, err
	}
	if meta.size == 0 {
		return -1, nil
	}

	// 构造数据部分的key
	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	value, err := rds.db.Get(zk.encodeWithMember())
	if err != nil {
		return -1, err
	}

	return utils.FloatFromBytes(value), nil
}
