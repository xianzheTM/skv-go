package data

import "encoding/binary"

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDelete
)

// crc type keySize valueSize
// 4 + 1 + 5 + 5 = 15
const maxLogRecordHeaderSize = 4 + 1 + binary.MaxVarintLen32*2

// LogRecord 数据日志记录
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// logRecordHeader 日志记录头部
type logRecordHeader struct {
	crc       uint32
	typ       LogRecordType
	keySize   uint32
	valueSize uint32
}

// LogRecordPos 描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 // 文件ID，表示数据存储到了哪个文件
	Offset int64  // 数据在文件中的偏移
}

// EncodeLogRecord 编码日志记录
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	return nil, 0
}

// decodeLogRecordHeader 解码日志记录头部，注意传入的字节切片可能会比实际的头部大，结果中会返回实际的头部字节大小
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	return nil, 0
}
