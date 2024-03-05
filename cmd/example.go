package main

import (
	"strconv"

	"github.com/ISSuh/lsm-tree/storage"
)

const (
	Num = 10000000
)

func main() {
	option := storage.NewOption()
	option.Path = "./out"

	s := storage.NewStorage(option)

	for i := 0; i <= Num; i++ {
		key := strconv.Itoa(i)
		value := []byte(strconv.Itoa(i))
		s.Set(key, value)
	}
}
