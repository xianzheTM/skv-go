package data

import "skv-go/fio"

// DataFile 数据文件
type DataFile struct {
	//文件id
	FileId uint32
	//文件写到的位置
	WriteOff  int64
	IOManager fio.IOManager
}

func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	return nil, nil
}

func (df *DataFile) Read(offset int64) (*LogRecord, error) {
	return nil, nil
}

func (df *DataFile) Write(bytes []byte) error {
	return nil
}

func (df *DataFile) Sync() error {
	return nil
}
