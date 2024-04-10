package skv_go

import "skv-go/index"

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
