package data

import (
	"fmt"
	"io"
	"path/filepath"
	"skv-go/fio"
)

const DataFileSuffix = ".data"

// DataFile 数据文件
type DataFile struct {
	//文件id
	FileId uint32
	//文件写到的位置
	WriteOff  int64
	IOManager fio.IOManager
}

func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileSuffix)
	ioManager, err := fio.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		FileId:    fileId,
		WriteOff:  0,
		IOManager: ioManager,
	}, nil
}

func (df *DataFile) Read(offset int64) (*LogRecord, int64, error) {
	fileSize, err := df.IOManager.Size()
	if err != nil {
		return nil, 0, err
	}
	headerByteSize := int64(maxLogRecordHeaderSize)
	if offset+maxLogRecordHeaderSize > fileSize {
		headerByteSize = fileSize - offset
	}
	//按头部最大长度读取头部
	headerBuf, err := df.readNBytes(headerByteSize, offset)
	if err != nil {
		return nil, 0, err
	}
	//解析头部，注意这里面要忽略掉多读的字节
	header, headerSize := decodeLogRecordHeader(headerBuf)
	if header == nil {
		return nil, 0, io.EOF
	}
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	var recordSize = headerSize + keySize + valueSize
	logRecord := &LogRecord{Type: header.typ}
	if keySize > 0 || valueSize > 0 {
		//读取出头部后面的实际的数据
		kvBuf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}
		//取出key和value
		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}
	crc := getLogRecordCRC(logRecord, headerBuf[:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}
	return logRecord, recordSize, nil
}

func (df *DataFile) Write(bytes []byte) error {
	n, err := df.IOManager.Write(bytes)
	if err != nil {
		return err
	}
	df.WriteOff += int64(n)
	return nil
}

func (df *DataFile) Sync() error {
	if err := df.IOManager.Sync(); err != nil {
		return err
	}
	return nil
}

func (df *DataFile) Close() error {
	if err := df.IOManager.Close(); err != nil {
		return err
	}
	return nil
}

func (df *DataFile) readNBytes(n int64, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = df.IOManager.Read(b, offset)
	return
}
