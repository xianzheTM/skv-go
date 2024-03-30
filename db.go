package skv_go

import (
	"skv-go/data"
	"skv-go/index"
	"sync"
)

type DB struct {
	options Options
	rw      *sync.RWMutex
	//当前活跃的数据文件
	activeFile *data.DataFile
	//旧的数据文件，仅用于读取
	olderFiles map[uint32]*data.DataFile
	//内存索引
	index index.Indexer
}

// Put 写入Key/Value数据，Key不能为空
func (db *DB) Put(key []byte, value []byte) error {
	//判断key是否为空
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	//构造logRecord
	logRecord := data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}
	pos, err := db.appendLogRecord(&logRecord)
	if err != nil {
		return err
	}
	//更新内存索引
	if db.index.Put(key, pos) {
		return nil
	} else {
		return ErrIndexUpdate
	}
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.rw.RLock()
	defer db.rw.RUnlock()

	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	pos := db.index.Get(key)
	if pos == nil {
		return nil, ErrKeyNotFound
	}
	//找到数据文件
	var dataFile *data.DataFile
	if pos.Fid == db.activeFile.FileId {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[pos.Fid]
	}
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}
	//根据偏移量读取数据
	logRecord, err := dataFile.Read(pos.Offset)
	if err != nil {
		return nil, err
	}
	if logRecord.Type == data.LogRecordDelete {
		return nil, ErrDataDeleted

	}

	return logRecord.Value, nil

}

// appendLogRecord 追加一条日志记录，并返回日志记录的位置用于维护索引
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.rw.Lock()
	defer db.rw.Unlock()
	if db.activeFile == nil {
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}
	encRecode, size := data.EncodeLogRecord(logRecord)
	//如果写入的数据已经达到活跃文件的阈值，则将活跃文件设置为旧文件，并创建一个新的活跃文件
	if db.activeFile.WriteOff+size > db.options.DataFileSize {
		//先持久化数据文件
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		db.olderFiles[db.activeFile.FileId] = db.activeFile

		//创建新的活跃文件
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}

	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecode); err != nil {
		return nil, err
	}
	if db.options.SyncWrite {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}
	//构造内存索引信息
	return &data.LogRecordPos{
		Fid:    db.activeFile.FileId,
		Offset: writeOff,
	}, nil
}

// setActiveFile 设置当前活跃的数据文件，如果当前没有活跃的数据文件，则创建一个新的数据文件，并设置为活跃文件
// 使用该方法需要加锁
func (db *DB) setActiveFile() error {
	var initFileId uint32 = 0
	if db.activeFile != nil {
		initFileId = db.activeFile.FileId + 1
	}
	dataFile, err := data.OpenDataFile(db.options.DirPath, initFileId)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}
