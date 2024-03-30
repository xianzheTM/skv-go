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
