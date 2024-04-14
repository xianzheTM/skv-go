package utils

import (
	"testing"
	"unicode"
)

func TestRandomValueLength(t *testing.T) {
	length := 10
	value := RandomValue(length)
	if got := len(value); got != length+len(ValuePrefix) {
		t.Errorf("RandomValue() = %v, want %v", got, length+len(ValuePrefix))
	}
}

func TestRandomValueCharacters(t *testing.T) {
	length := 100
	value := RandomValue(length)
	for _, char := range value[len(ValuePrefix):] {
		if !unicode.IsLetter(rune(char)) && !unicode.IsNumber(rune(char)) {
			t.Errorf("RandomValue() contains invalid character: %v", char)
		}
	}
}

func TestGetTestKeyFormat(t *testing.T) {
	i := 123
	key := GetTestKey(i)
	expectedPrefix := KeyPrefix
	if string(key[:len(expectedPrefix)]) != expectedPrefix {
		t.Errorf("GetTestKey() prefix = %v, want %v", string(key[:len(expectedPrefix)]), expectedPrefix)
	}
}

func TestGetTestKeyNumber(t *testing.T) {
	i := 123
	key := GetTestKey(i)
	expectedSuffix := "000000123"
	if string(key[len(key)-len(expectedSuffix):]) != expectedSuffix {
		t.Errorf("GetTestKey() suffix = %v, want %v", string(key[len(key)-len(expectedSuffix):]), expectedSuffix)
	}
}
