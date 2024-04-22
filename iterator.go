package skv_go

import (
	"bytes"
	"skv-go/index"
)

type Iterator struct {
	indexIter index.Iterator  //索引迭代器
	db        *DB             //数据库实例
	options   IteratorOptions //迭代器配置项
}

// NewIterator 创建一个迭代器
func (db *DB) NewIterator(opts IteratorOptions) *Iterator {
	return &Iterator{
		indexIter: db.index.Iterator(opts.Reverse),
		db:        db,
		options:   opts,
	}
}

// Rewind 回到起点
func (it *Iterator) Rewind() {
	it.indexIter.Rewind()
	it.skipToNext()
}

// Seek 根据传入的key找到对应的位置
func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
	it.skipToNext()
}

// Next 移动到下一个位置
func (it *Iterator) Next() {
	it.indexIter.Next()
	it.skipToNext()
}

// Valid 判断是否有效，即是否还有下一个位置
func (it *Iterator) Valid() bool {
	return it.indexIter.Valid()
}

// Key 获取key
func (it *Iterator) Key() []byte {
	return it.indexIter.Key()
}

// Value 获取value
func (it *Iterator) Value() ([]byte, error) {
	logRecordPos := it.indexIter.Value()
	it.db.rw.RLock()
	defer it.db.rw.RUnlock()
	return it.db.getValueByPosition(logRecordPos)
}

// Close 关闭迭代器
func (it *Iterator) Close() {
	it.indexIter.Close()
}

// skipToNext 跳过前缀不匹配的key
func (it *Iterator) skipToNext() {
	prefixLen := len(it.options.Prefix)
	if prefixLen == 0 {
		return
	}
	for ; it.indexIter.Valid(); it.indexIter.Next() {
		key := it.indexIter.Key()
		//如果key的前缀不是指定的前缀，就跳过
		if prefixLen <= len(key) && bytes.Compare(it.options.Prefix, key[:prefixLen]) == 0 {
			break
		}
	}
}
