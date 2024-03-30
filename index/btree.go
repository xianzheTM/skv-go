package index

import (
	"github.com/google/btree"
	"skv-go/data"
	"sync"
)

// BTree索引实现，封装一下
type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

func NewBTree() *BTree {
	return &BTree{btree.New(32), new(sync.RWMutex)}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	item := Item{
		key: key,
		pos: pos,
	}
	bt.lock.Lock()
	bt.tree.ReplaceOrInsert(&item)
	bt.lock.Unlock()
	return true
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	item := Item{key: key}
	btreeItem := bt.tree.Get(&item)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}

func (bt *BTree) Delete(key []byte) bool {
	item := Item{key: key}
	bt.lock.Lock()
	old := bt.tree.Delete(&item)
	bt.lock.Unlock()
	if old == nil {
		return false
	} else {
		return true
	}
}
