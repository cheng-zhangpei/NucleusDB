package fio

const DataFilePerm = 0644

type FileIOType = byte

const (
	// StandardFIO 标准文件 IO
	StandardFIO FileIOType = iota

	// MemoryMap 内存文件映射
	MemoryMap
)

// IOManager 抽象IO管理器,可以接入不同的IO类型目前支持标准文件IO，IO管理提供了一个面向数据文件读写的接口
type IOManager interface {
	// Read 从文件中读取数据
	Read([]byte, int64) (int, error)
	// Write 写入字节数据到文件中
	Write([]byte) (int, error)
	// Sync 持久化数据
	Sync() error
	// Close 关闭文件
	Close() error
	// Size 获取文件大小
	Size() (int64, error)
}

// 初始化IOManager，目前只有FileIO
func NewIOManager(fileName string, ioType FileIOType) (IOManager, error) {
	switch ioType {
	case StandardFIO:
		return NewFileIOManager(fileName)
	case MemoryMap:
		return NewMMapIOManager(fileName)
	default:
		panic("unsupported io type")
	}
}
