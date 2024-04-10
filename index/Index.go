package index

import (
	"bytes"
	"github.com/google/btree"
	"skv-go/data"
)

// 抽象的索引结构，用于索引的增删改查，而索引的实现可以是B树，红黑树等等，对应论文的keydir
type Indexer interface {
	// 插入索引，返回是否插入成功
	Put(key []byte, pos *data.LogRecordPos) bool
	// 查询索引，返回数据在文件中的位置
	Get(key []byte) *data.LogRecordPos
	// 删除索引，返回是否删除成功
	Delete(key []byte) bool
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
