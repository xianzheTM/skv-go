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

// IteratorOptions 迭代器配置项
type IteratorOptions struct {
	//遍历前缀为指定值的key
	Prefix []byte
	//是否反向遍历，默认false为正向的
	Reverse bool
}

var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024,
	SyncWrite:    false,
	IndexType:    index.BTreeIndex,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}
