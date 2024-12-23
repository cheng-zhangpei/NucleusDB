package fio

import (
	"os"
)

func NewFileIOManager(fileName string) (*FileIo, error) {
	fd, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, DataFilePerm)
	if err != nil {
		return nil, err
	}
	return &FileIo{fd: fd}, nil
}

// FileIo 标准系统文件,此处需要对IO类型进行封装
type FileIo struct {
	fd *os.File // 系统文件表述符
}

// Read 从文件中读取数据
func (fio *FileIo) Read(b []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(b, offset)
}

// Write 写入字节数据到文件中
func (fio *FileIo) Write(b []byte) (int, error) {
	return fio.fd.Write(b)
}

// Sync 持久化数据
func (fio *FileIo) Sync() error {
	return fio.fd.Sync()
}

// Close 关闭文件
func (fio *FileIo) Close() error {
	return fio.fd.Close()
}
func (fio *FileIo) Size() (int64, error) {
	stat, err := fio.fd.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}
