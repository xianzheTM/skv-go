package data

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDelete
)

// LogRecord 数据日志记录
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// LogRecordPos 描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 // 文件ID，表示数据存储到了哪个文件
	Offset int64  // 数据在文件中的偏移
}

func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	return nil, 0
}
