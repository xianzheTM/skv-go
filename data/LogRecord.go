package data

type LogRecordPos struct {
	Fid    uint32 // 文件ID，表示数据存储到了哪个文件
	Offset int64  // 数据在文件中的偏移
}
