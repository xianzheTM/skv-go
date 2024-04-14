package utils

import (
	"fmt"
	"math/rand"
	"time"
)

var (
	charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))

	KeyPrefix = "skv-go-key"

	ValuePrefix = "skv-go-value"
)

func GetTestKey(i int) []byte {
	return []byte(fmt.Sprintf(KeyPrefix+"%09d", i))
}

// RandomValue 生成指定长度的value
func RandomValue(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return []byte(ValuePrefix + string(b))
}
