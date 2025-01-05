package data

import (
	"ComDB/fio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
)

const DataFileNameSuffix = ".data"
const HintFileName = "hint-index"
const MergeFinishedFileName = "merge-finished"
const SeqNoFileName = "seq-no"

// crc type keySize valueSize 4 + 1 + 5 + 5 = 15 binary.MaxVarintLen32其实标识的就是两个变化大小的常量key、value的size
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

var (
	ErrInvalidedCRC = errors.New("invalided crc value, log record maybe corrupted")
)

// DataFile 数据文件的字段
type DataFile struct {
	FileId    uint32        // 文件id
	WritOff   int64         //偏移
	IOManager fio.IOManager // 对文件进行实际的读写操作
}

// OpenDataFile 初始化并打开数据文件
func OpenDataFile(dirPath string, fileId uint32, ioTYpe fio.FileIOType) (*DataFile, error) {
	fileName := GetDataFileName(dirPath, fileId)
	return NewDataFile(fileName, fileId, ioTYpe)
}

// OpenHintDataFile 打开hint文件
func OpenHintDataFile(fileName string, ioTYpe fio.FileIOType) (*DataFile, error) {
	return NewDataFile(fileName, 0, ioTYpe)
}

// OpenMergeFinishedFile 打开标识merge fin的文件
func OpenMergeFinishedFile(dirPath string, ioTYpe fio.FileIOType) (*DataFile, error) {
	fileName := filepath.Join(dirPath, MergeFinishedFileName)
	return NewDataFile(fileName, 0, ioTYpe)
}

// OpenSeqNoFile 打开一个序列号文件
func OpenSeqNoFile(dirPath string, ioTYpe fio.FileIOType) (*DataFile, error) {
	return NewDataFile(filepath.Join(dirPath, SeqNoFileName), 0, ioTYpe)
}

func GetDataFileName(dirPath string, fileId uint32) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)
}

func NewDataFile(fileName string, fileId uint32, ioTYpe fio.FileIOType) (*DataFile, error) {
	ioManager, err := fio.NewIOManager(fileName, ioTYpe)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		FileId:    fileId,
		WritOff:   0,
		IOManager: ioManager,
	}, nil
}

func (df *DataFile) Sync() error {
	return df.IOManager.Sync()
}
func (df *DataFile) Close() error {
	return df.IOManager.Close()
}
func (df *DataFile) Write(buf []byte) error {
	n, err := df.IOManager.Write(buf)
	if err != nil {
		return err
	}
	df.WritOff += int64(n)
	// 这个地方有一个很小心的地方我们每次读取的时候都是读取MaxHeaderSize这样会导致如果最后一个位置的数据不到axHeaderSize会出现异常
	return nil
}

func (df *DataFile) WriteHintFile(key []byte, pos *LogRecordPos) error {

	record := &LogRecord{
		Key: key,
		// 涉及到写入的操作肯定是需要对LogRecordPos进行编码的
		Value: EncodeLogRecordPos(pos),
	}
	encLogRecord, _ := EncodeLogRecord(record)
	return df.Write(encLogRecord)
}

// ReadLogRecord 根据偏移读取具体的LogRecord==> 自己写go的习惯一直不是非常好，这个地方要记得自己
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	fileSize, err := df.IOManager.Size()
	if err != nil {
		return nil, 0, err
	}
	var headerBytes int64 = maxLogRecordHeaderSize
	if offset+maxLogRecordHeaderSize > fileSize {
		// 也就是如果最后一个文件非常小，甚至没有我们所定义的最大的头部还要小，这个时候是没办法一口气读取出来15个字节的
		// 这里就从文件末尾往前走offset个位置定位新的头部
		headerBytes = fileSize - offset
	}

	// 将header给读出来,此处读出来的是字节的原始的大小
	headerBuf, err := df.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}
	header, headerSize := DecodeLogRecordHeader(headerBuf)
	// 两种读取到了文件末尾的情况，(1) header为空 (2) crc == 0 .....
	if header == nil {
		return nil, 0, io.EOF
	}
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}
	// 取出key和value的长度
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	var recordSize = keySize + valueSize + headerSize
	// 开始读取用户实际存储的键值对
	logRecord := &LogRecord{Type: header.recordType}

	// 解出key value
	if keySize > 0 || valueSize > 0 {
		kvBuf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}
		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}
	// 校验CRC是否正确
	// 这个地方需要注意一个细节：headerBuf的整个长度是之前所标识的变长的最大的长度，所以这里不能传入整个最大的长度
	crc := getLogRecordCRC(logRecord, headerBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidedCRC
	}

	return logRecord, recordSize, nil
}

func (df *DataFile) readNBytes(n int64, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = df.IOManager.Read(b, offset)
	return
}

func (df *DataFile) SetIOManager(dirPath string, ioType fio.FileIOType) error {
	if err := df.IOManager.Close(); err != nil {
		return err
	}
	ioManager, err := fio.NewIOManager(GetDataFileName(dirPath, df.FileId), ioType)
	if err != nil {
		return err
	}
	df.IOManager = ioManager
	return nil
}
