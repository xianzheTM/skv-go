package index

import (
	"bytes"
	"github.com/google/btree"
	"skv-go/data"
)

// Indexer 抽象的索引结构，用于索引的增删改查，而索引的实现可以是B树，红黑树等等，对应论文的keydir
type Indexer interface {
	// Put 插入索引，返回是否插入成功
	Put(key []byte, pos *data.LogRecordPos) bool
	// Get 查询索引，返回数据在文件中的位置
	Get(key []byte) *data.LogRecordPos
	// Delete 删除索引，返回是否删除成功
	Delete(key []byte) bool
	// Size 获取索引的大小
	Size() int
	// Iterator 获取迭代器
	Iterator(reverse bool) Iterator
}

type IndexType = int8

const (
	BTreeIndex IndexType = iota

	ART
)

func NewIndexer(typ IndexType) Indexer {
	switch typ {
	case BTreeIndex:
		return NewBTree()
	case ART:
		return nil
	default:
		panic("unknown index type")
	}
	return nil

}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}

// Iterator 迭代器接口
type Iterator interface {
	// Rewind 回到起点
	Rewind()
	// Seek 根据传入的key找到对应的位置
	Seek(key []byte)
	// Next 移动到下一个位置
	Next()
	// Valid 判断是否有效，即是否还有下一个位置
	Valid() bool
	// Key 获取当前位置的key
	Key() []byte
	// Value 获取当前位置的value
	Value() *data.LogRecordPos
	// Close 关闭迭代器
	Close()
}
