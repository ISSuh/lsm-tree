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

	builder := table.NewTableBuilder(24)
	builder.Add([]byte("aaa"), []byte("aaa"))
	builder.Add([]byte("bbb"), []byte("bbb"))
	builder.Add([]byte("ccc"), []byte("ccc"))
	builder.Add([]byte("ddd"), []byte("ddd"))
	builder.Add([]byte("eee"), []byte("eee"))
	builder.Add([]byte("fff"), []byte("fff"))

	builder.BuildTable(0, "./test")
}
