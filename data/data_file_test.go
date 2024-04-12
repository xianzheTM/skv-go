package data

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWrite(t *testing.T) {
	dir, _ := os.MkdirTemp("", "test")
	defer os.RemoveAll(dir)

	df, _ := OpenDataFile(dir, 1)
	defer df.Close()

	// Write a log record
	logRecord := &LogRecord{
		Key:   []byte("Hello"),
		Value: []byte("world"),
		Type:  LogRecordNormal,
	}
	encLogRecord, _ := EncodeLogRecord(logRecord)
	err := df.Write(encLogRecord)
	assert.NoError(t, err)
}

func TestSync(t *testing.T) {
	dir, _ := os.MkdirTemp("", "test")
	defer os.RemoveAll(dir)

	df, _ := OpenDataFile(dir, 1)
	defer df.Close()

	// Sync the data file
	err := df.Sync()
	assert.NoError(t, err)
}

func TestClose(t *testing.T) {
	dir, _ := os.MkdirTemp("", "test")
	defer os.RemoveAll(dir)

	df, _ := OpenDataFile(dir, 1)

	// Close the data file
	err := df.Close()
	assert.NoError(t, err)
}

func TestIntegration(t *testing.T) {
	dir, _ := os.MkdirTemp("", "test")
	defer os.RemoveAll(dir)

	df, _ := OpenDataFile(dir, 1)

	// Write a log record
	logRecord := &LogRecord{
		Key:   []byte("Hello"),
		Value: []byte("world"),
		Type:  LogRecordNormal,
	}
	encLogRecord, _ := EncodeLogRecord(logRecord)
	err := df.Write(encLogRecord)
	assert.NoError(t, err)

	// Sync the data file
	err = df.Sync()
	assert.NoError(t, err)

	// Close the data file
	err = df.Close()
	assert.NoError(t, err)

	// Reopen the data file
	df, _ = OpenDataFile(dir, 1)
	defer df.Close()
}
