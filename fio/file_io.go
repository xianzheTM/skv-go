package fio

import "os"

// FileIO 标准系统文件IO操作
type FileIO struct {
	//文件描述符
	fd *os.File
}

func NewFileIOManager(fileName string) (*FileIO, error) {
	file, err := os.OpenFile(
		fileName,
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		DataFilePerm)
	if err != nil {
		return nil, err
	}
	return &FileIO{fd: file}, nil
}

func (fio *FileIO) Read(bytes []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(bytes, offset)
}

func (fio *FileIO) Write(bytes []byte) (int, error) {
	return fio.fd.Write(bytes)
}

func (fio *FileIO) Sync() error {
	return fio.fd.Sync()
}

func (fio *FileIO) Close() error {
	return fio.fd.Close()
}
