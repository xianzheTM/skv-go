package fio

const DataFilePerm = 0644

type IOManager interface {
	// Read 从文件的给定位置读取数据
	Read([]byte, int64) (int, error)
	// Write 将数据写入文件的给定位置
	Write([]byte) (int, error)
	// Sync 同步文件
	Sync() error
	// Close 关闭文件
	Close() error
}
