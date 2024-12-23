package fio

const DataFilePerm = 0644

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
func NewIOManager(fileName string) (IOManager, error) {
	return NewFileIOManager(fileName)
}
