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
	offset int16
}

func main() {
	fmt.Println("TEST")

	builder := table.NewTableBuilder(20, 100)
	builder.Add([]byte("aaaa"), []byte("aaaa"))
	builder.Add([]byte("bbbb"), []byte("bbbb"))
	builder.Add([]byte("cccc"), []byte("cccc"))
	builder.Add([]byte("dddd"), []byte("dddd"))

	temp := builder.BuildTable(0, "./test")
	fmt.Println(temp)

}
