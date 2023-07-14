package main

import (
	"fmt"

	table "github.com/ISSuh/lsm-tree/table"
)

const (
	DefaultMaxBlockSize = 4096
	DefaultMaxTableSize = DefaultMaxBlockSize * 10
)

type Entry struct {
}

func main() {
	fmt.Println("TEST")

	builder := table.NewTableBuilder(DefaultMaxBlockSize, DefaultMaxTableSize)
	builder.Add([]byte("test"), []byte("test"))

	temp := builder.BuildTable(0, "./")
	fmt.Println(temp)

}
