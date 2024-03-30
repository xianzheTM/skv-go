package fio_test

import (
	"os"
	"skv-go/fio"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewFileIOManager(t *testing.T) {
	tempFile, _ := os.CreateTemp("", "test")
	defer os.Remove(tempFile.Name())

	fileIO, err := fio.NewFileIOManager(tempFile.Name())

	assert.NoError(t, err)
	assert.NotNil(t, fileIO)
}

func TestFileIO_Read(t *testing.T) {
	tempFile, _ := os.CreateTemp("", "test")
	defer os.Remove(tempFile.Name())
	tempFile.WriteString("Hello, World!")

	fileIO, _ := fio.NewFileIOManager(tempFile.Name())
	bytes := make([]byte, 5)

	n, err := fileIO.Read(bytes, 0)

	assert.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, "Hello", string(bytes))
}

func TestFileIO_Write(t *testing.T) {
	tempFile, _ := os.CreateTemp("", "test")
	defer os.Remove(tempFile.Name())

	fileIO, _ := fio.NewFileIOManager(tempFile.Name())
	bytes := []byte("Hello, World!")

	n, err := fileIO.Write(bytes)

	assert.NoError(t, err)
	assert.Equal(t, len(bytes), n)

	content, _ := os.ReadFile(tempFile.Name())
	assert.Equal(t, "Hello, World!", string(content))
}

func TestFileIO_Sync(t *testing.T) {
	tempFile, _ := os.CreateTemp("", "test")
	defer os.Remove(tempFile.Name())

	fileIO, _ := fio.NewFileIOManager(tempFile.Name())

	err := fileIO.Sync()

	assert.NoError(t, err)
}

func TestFileIO_Close(t *testing.T) {
	tempFile, _ := os.CreateTemp("", "test")
	defer os.Remove(tempFile.Name())

	fileIO, _ := fio.NewFileIOManager(tempFile.Name())

	err := fileIO.Close()

	assert.NoError(t, err)
}

func TestFileIO_Integration(t *testing.T) {
	tempFile, _ := os.CreateTemp("", "test")
	defer os.Remove(tempFile.Name())

	fileIO, err := fio.NewFileIOManager(tempFile.Name())
	assert.NoError(t, err)

	// Write to the file
	bytes := []byte("Hello, World!")
	n, err := fileIO.Write(bytes)
	assert.NoError(t, err)
	assert.Equal(t, len(bytes), n)

	// Sync the file
	err = fileIO.Sync()
	assert.NoError(t, err)

	// Read from the file
	readBytes := make([]byte, len(bytes))
	n, err = fileIO.Read(readBytes, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(bytes), n)
	assert.Equal(t, bytes, readBytes)

	// Close the file
	err = fileIO.Close()
	assert.NoError(t, err)
}
