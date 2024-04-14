package main

import (
	"fmt"
	skv "skv-go"
)

func main() {
	options := skv.DefaultOptions
	db, err := skv.Open(options)
	if err != nil {
		panic(err)
	}
	err = db.Put([]byte("key"), []byte("value"))
	if err != nil {
		panic(err)
	}
	value, err := db.Get([]byte("key"))
	fmt.Printf("value: %s\n", value)

}
