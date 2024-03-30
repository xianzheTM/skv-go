package skv_go

type Options struct {
	// 数据文件目录
	DirPath string
	//数据文件大小
	DataFileSize int64
	//每次写入是否持久化
	SyncWrite bool
}
