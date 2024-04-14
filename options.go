package skv_go

import (
	"os"
	"skv-go/index"
)

type Options struct {
	// 数据文件目录
	DirPath string
	//数据文件大小
	DataFileSize int64
	//每次写入是否持久化
	SyncWrite bool
	//索引类型
	IndexType index.IndexType
}

var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024,
	SyncWrite:    false,
	IndexType:    index.BTreeIndex,
}
