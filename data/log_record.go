package data

import (
	"encoding/binary"
	"hash/crc32"
)

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

// EncodeLogRecord 编码日志记录，由CRC，type，keySize，valueSize，key，value组成
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	header := make([]byte, maxLogRecordHeaderSize)
	//前四个字节为CRC，需要最后计算
	var index = 0
	index += crc32.Size
	header[index] = logRecord.Type
	index = 5
	//从5开始存储keySize和valueSize
	index += binary.PutUvarint(header[index:], uint64(uint32(len(logRecord.Key))))
	index += binary.PutUvarint(header[index:], uint64(int64(len(logRecord.Value))))
	var size = index + len(logRecord.Key) + len(logRecord.Value)
	encBytes := make([]byte, size)
	//将header和key，value拷贝到encBytes中
	copy(encBytes[:index], header[:index])
	copy(encBytes[index:], logRecord.Key)
	copy(encBytes[index+len(logRecord.Key):], logRecord.Value)
	//计算CRC
	crc := crc32.ChecksumIEEE(encBytes[crc32.Size:])
	binary.LittleEndian.PutUint32(encBytes[:crc32.Size], crc)
	return encBytes, int64(size)
}

// decodeLogRecordHeader 解码日志记录头部，注意传入的字节切片可能会比实际的头部大，结果中会返回实际的头部字节大小
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	if len(buf) <= crc32.Size {
		return nil, 0
	}
	header := &logRecordHeader{
		crc: binary.LittleEndian.Uint32(buf[:crc32.Size]),
		typ: buf[crc32.Size],
	}
	var index = 5
	//取出实际的keySize和valueSize
	keySize, keySizeLen := binary.Uvarint(buf[index:])
	header.keySize = uint32(keySize)
	index += keySizeLen
	valueSize, valueSizeLen := binary.Uvarint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += valueSizeLen
	return header, int64(index)
}

// getLogRecordCRC 计算日志记录的CRC
func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	if lr == nil {
		return 0
	}
	//先计算header的crc，注意剔除掉头部中的crc
	crc := crc32.ChecksumIEEE(header[crc32.Size:])
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)
	return crc
}
