package skv_go

import (
	"errors"
	"io"
	"log"
	"os"
	"skv-go/data"
	"skv-go/index"
	"sort"
	"strconv"
	"strings"
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
	//文件id列表，仅在加载索引时使用
	fileIds []uint32
}

// Open 打开数据库实例
func Open(options Options) (*DB, error) {
	//校验配置项
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	//如果配置项中的文件路径不存在，则创建
	if _, err := os.Stat(options.DirPath); err != nil {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	//初始化DB实例结构体
	db := &DB{
		options:    options,
		rw:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType),
	}

	//加载数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}
	return db, nil
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

// Get 读取Key对应的Value，Key不能为空
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
	logRecord, _, err := dataFile.Read(pos.Offset)
	if err != nil {
		return nil, err
	}
	if logRecord.Type == data.LogRecordDelete {
		return nil, ErrDataDeleted

	}

	return logRecord.Value, nil
}

// Delete 删除一条数据，Key不能为空
func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	//判断key是否存在
	if db.index.Get(key) == nil {
		return nil
	}

	logRecord := data.LogRecord{
		Key:  key,
		Type: data.LogRecordDelete,
	}
	_, err := db.appendLogRecord(&logRecord)
	if err != nil {
		return err
	}
	if !db.index.Delete(key) {
		return ErrIndexUpdate
	}
	return nil
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
		log.Print("active file is full, create a new one")
		//先持久化数据文件
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		db.olderFiles[db.activeFile.FileId] = db.activeFile

		//创建新活跃文件
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

// loadDataFiles 加载数据文件
func (db *DB) loadDataFiles() error {
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}
	var fileIds []uint32
	//遍历目录中的所有文件，找到以.data结尾的文件
	for _, dirEntry := range dirEntries {
		if strings.HasSuffix(dirEntry.Name(), data.DataFileSuffix) {
			splitName := strings.Split(dirEntry.Name(), ".")
			fileId, err := strconv.Atoi(splitName[0])
			if err != nil {
				return ErrDataDirCorrupt
			}
			fileIds = append(fileIds, uint32(fileId))
		}
	}
	//对fileIds进行排序
	sort.Slice(fileIds, func(i, j int) bool {
		return fileIds[i] < fileIds[j]
	})
	db.fileIds = fileIds
	//加载数据文件
	for i, fileId := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DirPath, fileId)
		if err != nil {
			return err
		}
		if i == len(fileIds)-1 {
			db.activeFile = dataFile
		} else {
			db.olderFiles[fileId] = dataFile
		}
	}
	return nil
}

// loadIndexFromDataFiles 加载索引数据文件
func (db *DB) loadIndexFromDataFiles() error {
	if len(db.fileIds) == 0 {
		return nil
	}
	//遍历文件id，取出文件中的记录
	for _, fileId := range db.fileIds {
		var dataFile *data.DataFile
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}

		//读取dataFile中的所有内容
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.Read(offset)
			if err != nil {
				//如果读取到文件末尾，则跳出循环
				if err == io.EOF {
					break
				}
				return err
			}
			//构造内存索引
			logRecordPos := data.LogRecordPos{
				Fid:    fileId,
				Offset: offset,
			}
			if logRecord.Type == data.LogRecordDelete {
				db.index.Delete(logRecord.Key)
			} else {
				db.index.Put(logRecord.Key, &logRecordPos)
			}
			//更新offset
			offset += size
		}

		//如果是活跃文件，则更新db中的写入偏移量
		if fileId == db.activeFile.FileId {
			db.activeFile.WriteOff = offset
		}
	}
	return nil
}

// checkOptions 校验配置项
func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("DirPath is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("DataFileSize is invalid")
	}
	return nil
}
