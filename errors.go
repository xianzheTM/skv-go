package skv_go

import "errors"

var (
	ErrKeyIsEmpty       = errors.New("key is empty")
	ErrIndexUpdate      = errors.New("index update error")
	ErrKeyNotFound      = errors.New("key not found")
	ErrDataFileNotFound = errors.New("data file error")
	ErrDataDeleted      = errors.New("data deleted")
)
