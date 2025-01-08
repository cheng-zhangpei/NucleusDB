package redis

import (
	"ComDB/utils"
	"encoding/binary"
	"math"
)

const (
	maxMetaDateSize   = 1 + binary.MaxVarintLen64*2 + binary.MaxVarintLen32
	extraListMetaSize = binary.MaxVarintLen64 * 2

	initialListMark = math.MaxUint64
)

type metadata struct {
	dataType byte   //数据类型
	expire   int64  // 过期时间
	version  int64  // 版本号
	size     uint32 // List专有结构
	head     uint64 // List专用
	tail     uint64 // List专用
}

func (md *metadata) encode() []byte {
	var size = maxMetaDateSize
	if md.dataType == List {
		size += extraListMetaSize
	}
	buf := make([]byte, size)
	buf[0] = md.dataType

	var index = 1
	index += binary.PutVarint(buf[index:], md.expire)
	index += binary.PutVarint(buf[index:], md.version)
	index += binary.PutVarint(buf[index:], int64(md.size))

	if md.dataType == List {
		index += binary.PutUvarint(buf[index:], md.head)
		index += binary.PutUvarint(buf[index:], md.tail)
	}
	return buf[:index]
}

func decodeMetaData(buf []byte) *metadata {
	dataType := buf[0]

	var index = 1
	expire, n := binary.Varint(buf[index:])
	index += n
	version, n := binary.Varint(buf[index:])
	index += n
	size, n := binary.Varint(buf[index:])
	var head uint64 = 0
	var tail uint64 = 0
	index += n
	if dataType == List {
		head, n = binary.Uvarint(buf[index:])
		index += n
		tail, n = binary.Uvarint(buf[index:])
		index += n
	}
	return &metadata{
		dataType: dataType,
		expire:   expire,
		version:  version,
		size:     uint32(size),
		head:     head,
		tail:     tail,
	}
}

type hashInternalKey struct {
	key     []byte
	version int64
	field   []byte
}
type ListInternalKey struct {
	key     []byte
	version int64
	index   uint64
}
type SetInternalKey struct {
	key     []byte
	version int64
	member  []byte
}

func (hk *hashInternalKey) encode() []byte {
	buf := make([]byte, len(hk.key)+len(hk.field)+8)
	// key
	var index = 0
	copy(buf[index:index+len(hk.key)], hk.key)
	index += len(hk.key)

	//version

	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(hk.version))
	index += 8
	// field

	copy(buf[index:], hk.field)
	index += len(hk.field)

	return buf
}

func (lk *ListInternalKey) encode() []byte {
	buf := make([]byte, len(lk.key)+8+8)
	var index = 0

	copy(buf[index:index+len(lk.key)], lk.key)
	index += len(lk.key)

	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(lk.version))
	index += 8

	binary.LittleEndian.PutUint64(buf[index:], lk.index)
	return buf
}

func (sk *SetInternalKey) encode() []byte {
	buf := make([]byte, len(sk.key)+len(sk.member)+8+4)
	// key
	var index = 0
	copy(buf[index:index+len(sk.key)], sk.key)
	index += len(sk.key)

	//version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(sk.version))
	index += 8
	// member

	copy(buf[index:], sk.member)
	index += len(sk.member)
	// 将member的长度编码进去
	binary.LittleEndian.PutUint32(buf[index:], uint32(len(sk.member)))

	return buf
}

type zsetInternalKey struct {
	key     []byte
	version int64
	member  []byte
	score   float64
}

func (zk *zsetInternalKey) encodeWithMember() []byte {
	buf := make([]byte, len(zk.key)+len(zk.member)+8)

	// key
	var index = 0
	copy(buf[index:index+len(zk.key)], zk.key)
	index += len(zk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(zk.version))
	index += 8

	// member
	copy(buf[index:], zk.member)

	return buf
}

func (zk *zsetInternalKey) encodeWithScore() []byte {
	scoreBuf := utils.Float64ToBytes(zk.score)
	buf := make([]byte, len(zk.key)+len(zk.member)+len(scoreBuf)+8+4)

	// key
	var index = 0
	copy(buf[index:index+len(zk.key)], zk.key)
	index += len(zk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(zk.version))
	index += 8

	// score
	copy(buf[index:index+len(scoreBuf)], scoreBuf)
	index += len(scoreBuf)

	// member
	copy(buf[index:index+len(zk.member)], zk.member)
	index += len(zk.member)

	// member size
	binary.LittleEndian.PutUint32(buf[index:], uint32(len(zk.member)))

	return buf
}
