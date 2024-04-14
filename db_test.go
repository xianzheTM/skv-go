package skv_go

import (
	"fmt"
	"os"
	"skv-go/utils"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPut(t *testing.T) {
	dir, _ := os.MkdirTemp("", "test")

	options := DefaultOptions
	options.DataFileSize = 1024 * 1024
	options.DirPath = dir

	db, _ := Open(options)
	defer destoryDB(db)

	// Put a record
	err := db.Put(utils.GetTestKey(1), utils.RandomValue(5))
	assert.NoError(t, err)

	// Put a record with the same key
	err = db.Put([]byte("Hello"), []byte("universe"))
	assert.NoError(t, err)

	// Put a record with empty key and value
	err = db.Put([]byte(""), []byte(""))
	assert.Error(t, err)

	// Put a record when the active file size is not enough
	for i := 0; i < 300000; i++ {
		err = db.Put([]byte(utils.GetTestKey(i)), utils.RandomValue(3))
		assert.NoError(t, err)
	}

	// Put a record after switching to a new file
	err = db.Put([]byte("NewFile"), []byte("NewValue"))
	assert.NoError(t, err)
}

func TestGet(t *testing.T) {
	dir, _ := os.MkdirTemp("", "test")

	options := DefaultOptions
	options.DirPath = dir
	options.DataFileSize = 1024 * 1024

	db, _ := Open(options)

	// Put a record
	db.Put([]byte("Hello"), []byte("world"))

	// Get a record
	value, err := db.Get([]byte("Hello"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("world"), value)

	// Get a non-existent record
	_, err = db.Get([]byte("NonExistentKey"))
	assert.Error(t, err)

	// Get a record after it has been updated
	db.Put([]byte("Hello"), []byte("universe"))
	value, err = db.Get([]byte("Hello"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("universe"), value)

	// Get a record after it has been deleted
	db.Delete([]byte("Hello"))
	_, err = db.Get([]byte("Hello"))
	assert.Error(t, err)

	// Get a record from an old file after switching to a new file
	db.Put([]byte("Hello"), []byte("universe"))
	value, err = db.Get([]byte("Hello"))
	for i := 0; i < 300000; i++ {
		db.Put([]byte("Key"+fmt.Sprint(i)), []byte("Value"+fmt.Sprint(i)))
	}
	value, err = db.Get([]byte("Hello"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("universe"), value)

	// Get a record after restarting the DB
	err = db.activeFile.Close()
	assert.NoError(t, err)
	db, _ = Open(options)
	value, err = db.Get([]byte("Hello"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("universe"), value)
	destoryDB(db)
}

func TestDelete(t *testing.T) {
	dir, _ := os.MkdirTemp("", "test")
	defer os.RemoveAll(dir)

	options := DefaultOptions
	options.DirPath = dir

	db, _ := Open(options)
	defer destoryDB(db)

	// Put a record
	db.Put([]byte("Hello"), []byte("world"))

	// Delete a record
	err := db.Delete([]byte("Hello"))
	assert.NoError(t, err)

	// Delete a record with an empty key
	err = db.Delete([]byte(""))
	assert.Error(t, err)

	// Delete a non-existent record
	err = db.Delete([]byte("NonExistentKey"))
	assert.NoError(t, err)

	// Put a record after it has been deleted
	err = db.Put([]byte("Hello"), []byte("world"))
	assert.NoError(t, err)
}

func destoryDB(db *DB) {
	if db != nil {
		if db.activeFile != nil {
			_ = db.activeFile.Close()
		}
		for _, olderFile := range db.olderFiles {
			_ = olderFile.Close()
		}
		err := os.RemoveAll(db.options.DirPath)
		if err != nil {
			panic(err)
		}
	}
}
