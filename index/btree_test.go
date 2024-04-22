package index

import (
	"github.com/stretchr/testify/assert"
	"skv-go/data"
	"testing"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBTree()
	key := []byte("key")
	pos := &data.LogRecordPos{Offset: 1, Fid: 1}

	t.Run("PutNewItem", func(t *testing.T) {
		assert.True(t, bt.Put(key, pos), "Failed to put new item")
	})

	t.Run("PutExistingItem", func(t *testing.T) {
		assert.True(t, bt.Put(key, pos), "Failed to put existing item")
	})
}

func TestBTree_Get(t *testing.T) {
	bt := NewBTree()
	key := []byte("key")
	pos := &data.LogRecordPos{Offset: 1, Fid: 1}
	bt.Put(key, pos)

	t.Run("GetExistingItem", func(t *testing.T) {
		assert.Equal(t, pos, bt.Get(key), "Get() returned wrong value")
	})

	t.Run("GetNonExistingItem", func(t *testing.T) {
		assert.Nil(t, bt.Get([]byte("nonExisting")), "Get() should return nil for non-existing item")
	})
}

func TestBTree_Delete(t *testing.T) {
	bt := NewBTree()
	key := []byte("key")
	pos := &data.LogRecordPos{Offset: 1, Fid: 1}
	bt.Put(key, pos)

	t.Run("DeleteExistingItem", func(t *testing.T) {
		assert.True(t, bt.Delete(key), "Failed to delete existing item")
	})

	t.Run("DeleteNonExistingItem", func(t *testing.T) {
		assert.False(t, bt.Delete([]byte("nonExisting")), "Should not delete non-existing item")
	})
}

func TestBTree_Integration(t *testing.T) {
	bt := NewBTree()
	key := []byte("key")
	pos := &data.LogRecordPos{Offset: 1, Fid: 1}

	t.Run("PutAndGetItem", func(t *testing.T) {
		assert.True(t, bt.Put(key, pos), "Failed to put new item")
		assert.Equal(t, pos, bt.Get(key), "Get() returned wrong value")
	})

	t.Run("PutAndDeleteItem", func(t *testing.T) {
		assert.True(t, bt.Put(key, pos), "Failed to put new item")
		assert.True(t, bt.Delete(key), "Failed to delete existing item")
		assert.False(t, bt.Delete(key), "Failed to delete existing item")
		assert.Nil(t, bt.Get(key), "Get() should return nil for deleted item")
	})
}

// 创建一个预填充的B树和对应的迭代器
func createBTreeAndIterator(t *testing.T) (*BTree, Iterator) {
	bt := NewBTree()
	for i := 0; i < 10; i++ {
		key := []byte{byte(i)}
		pos := &data.LogRecordPos{Offset: int64(i), Fid: uint32(i)}
		assert.True(t, bt.Put(key, pos))
	}
	iterator := bt.Iterator(false)
	return bt, iterator
}

// 测试Rewind方法
func TestBTreeIterator_Rewind(t *testing.T) {
	_, iterator := createBTreeAndIterator(t)
	iterator.Next()
	iterator.Rewind()
	assert.Equal(t, []byte{0}, iterator.Key())
}

// 测试Seek方法
func TestBTreeIterator_Seek(t *testing.T) {
	_, iterator := createBTreeAndIterator(t)
	iterator.Seek([]byte{5})
	assert.Equal(t, []byte{5}, iterator.Key())
}

// 测试Next方法
func TestBTreeIterator_Next(t *testing.T) {
	_, iterator := createBTreeAndIterator(t)
	iterator.Next()
	assert.Equal(t, []byte{1}, iterator.Key())
}

// 测试Valid方法
func TestBTreeIterator_Valid(t *testing.T) {
	_, iterator := createBTreeAndIterator(t)
	assert.True(t, iterator.Valid())
	for i := 0; i < 10; i++ {
		iterator.Next()
	}
	assert.False(t, iterator.Valid())
}

// 测试Key和Value方法
func TestBTreeIterator_Key_Value(t *testing.T) {
	_, iterator := createBTreeAndIterator(t)
	for i := 0; i < 10; i++ {
		assert.Equal(t, []byte{byte(i)}, iterator.Key())
		assert.Equal(t, &data.LogRecordPos{Offset: int64(i), Fid: uint32(i)}, iterator.Value())
		iterator.Next()
	}
}

// 测试Close方法
func TestBTreeIterator_Close(t *testing.T) {
	_, iterator := createBTreeAndIterator(t)
	iterator.Close()
	assert.False(t, iterator.Valid())
}

// 集成测试
func TestBTreeIterator_Integration(t *testing.T) {
	bt, iterator := createBTreeAndIterator(t)
	for i := 0; i < 10; i++ {
		assert.True(t, iterator.Valid())
		assert.Equal(t, []byte{byte(i)}, iterator.Key())
		assert.Equal(t, bt.Get(iterator.Key()), iterator.Value())
		iterator.Next()
	}
	assert.False(t, iterator.Valid())
	iterator.Close()
	assert.False(t, iterator.Valid())
}

// 测试倒序迭代器
func TestBTreeIterator_Reverse(t *testing.T) {
	bt, _ := createBTreeAndIterator(t)
	reverseIterator := bt.Iterator(true)
	for i := 9; i >= 0; i-- {
		assert.True(t, reverseIterator.Valid())
		assert.Equal(t, []byte{byte(i)}, reverseIterator.Key())
		assert.Equal(t, &data.LogRecordPos{Offset: int64(i), Fid: uint32(i)}, reverseIterator.Value())
		reverseIterator.Next()
	}
	assert.False(t, reverseIterator.Valid())
	reverseIterator.Close()
	assert.False(t, reverseIterator.Valid())
}

// 测试倒序Seek方法
func TestBTreeIterator_ReverseSeek(t *testing.T) {
	_, reverseIterator := createBTreeAndIterator(t)
	reverseIterator.Seek([]byte{5})
	assert.Equal(t, []byte{5}, reverseIterator.Key())
}
