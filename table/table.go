package table

import (
	"log"
	"os"

	block "github.com/ISSuh/lsm-tree/block"
)

type Table struct {
	id               int
	path             string
	file             *os.File
	blockMetas       []block.BlockMeta
	blockMetasOffset int
}

type TableBuilder struct {
	blockBuilder *block.BlockBuilder
	data         []byte
	blockMetas   []block.BlockMeta
	fistKeys     [][]byte
	maxBlockSize int
	maxTableSize int
}

func NewTableBuilder(maxBlockSize, maxTableSize int) *TableBuilder {
	return &TableBuilder{
		blockBuilder: block.NewBlockBuilder(maxBlockSize),
		data:         make([]byte, 0),
		blockMetas:   make([]block.BlockMeta, 0),
		fistKeys:     make([][]byte, 0),
		maxBlockSize: maxBlockSize,
		maxTableSize: maxTableSize,
	}
}

func (builder *TableBuilder) Size() int {
	return len(builder.data)
}

func (builder *TableBuilder) Add(key, value []byte) {
	if builder.blockBuilder.Empty() {
		builder.fistKeys = append(builder.fistKeys, key)
	}

	if builder.blockBuilder.Add(key, value) {
		return
	}

	builder.flushingBlock()

	// retry add
	builder.Add(key, value)
}

func (builder *TableBuilder) flushingBlock() {
	newBlock := builder.blockBuilder.BuildBlock()

	offset := int16(len(builder.data))
	firstKey := builder.fistKeys[len(builder.fistKeys)-1]
	newBlockMeta := block.NewBlockMeta(offset, firstKey)

	builder.blockMetas = append(builder.blockMetas, newBlockMeta)
	builder.data = append(builder.data, newBlock.Data()...)

	builder.blockBuilder.Clear()
}

func (builder *TableBuilder) BuildTable(id int, path string) *Table {
	file, err := os.Create(path)
	if err != nil {
		log.Println("file open")
		return nil
	}
	defer file.Close()

	n, err := file.Write(builder.data)
	if err != nil || n != len(builder.data) {
		log.Println("write error")
		return nil
	}

	offset := len(builder.data)
	for _, meta := range builder.blockMetas {
		buffer := meta.Encode()

	}

	return &Table{
		id:               id,
		path:             path,
		file:             file,
		blockMetas:       builder.blockMetas,
		blockMetasOffset: offset,
	}
}
