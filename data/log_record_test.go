package data

import (
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeDecodeLogRecord(t *testing.T) {
	// Create a new LogRecord
	originalRecord := &LogRecord{
		Key:   []byte("TestKey"),
		Value: []byte("TestValue"),
		Type:  LogRecordNormal,
	}

	// Encode the LogRecord
	encodedRecord, _ := EncodeLogRecord(originalRecord)

	// Decode the header of the encoded LogRecord
	decodedHeader, headerSize := decodeLogRecordHeader(encodedRecord)

	// Assert that the decoded header matches the original LogRecord
	assert.Equal(t, originalRecord.Type, decodedHeader.typ)
	assert.Equal(t, uint32(len(originalRecord.Key)), decodedHeader.keySize)
	assert.Equal(t, uint32(len(originalRecord.Value)), decodedHeader.valueSize)

	// Assert that the CRC is correct
	assert.Equal(t, getLogRecordCRC(originalRecord, encodedRecord[:headerSize]), decodedHeader.crc)
}

func TestEncodeLogRecordWithEmptyKeyAndValue(t *testing.T) {
	// Create a new LogRecord with empty key and value
	originalRecord := &LogRecord{
		Key:   []byte(""),
		Value: []byte(""),
		Type:  LogRecordNormal,
	}

	// Encode the LogRecord
	encodedRecord, _ := EncodeLogRecord(originalRecord)

	// Decode the header of the encoded LogRecord
	decodedHeader, headerSize := decodeLogRecordHeader(encodedRecord)

	// Assert that the decoded header matches the original LogRecord
	assert.Equal(t, originalRecord.Type, decodedHeader.typ)
	assert.Equal(t, uint32(len(originalRecord.Key)), decodedHeader.keySize)
	assert.Equal(t, uint32(len(originalRecord.Value)), decodedHeader.valueSize)

	// Assert that the CRC is correct
	assert.Equal(t, getLogRecordCRC(originalRecord, encodedRecord[:headerSize]), decodedHeader.crc)
}

func TestDecodeLogRecordHeaderWithInsufficientData(t *testing.T) {
	// Create a byte slice with insufficient data
	insufficientData := make([]byte, crc32.Size-1)

	// Try to decode the header
	decodedHeader, _ := decodeLogRecordHeader(insufficientData)

	// Assert that the decoded header is nil
	assert.Nil(t, decodedHeader)
}
