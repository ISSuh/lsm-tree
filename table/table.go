package table

import (
	"encoding/binary"
	"log"
	"os"
	"reflect"

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
		log.Println("add")
		return
	}

	builder.flushingBlock()

	// retry add
	builder.Add(key, value)
}

func (builder *TableBuilder) flushingBlock() {
	log.Println("flushing")

	newBlock := builder.blockBuilder.BuildBlock()
	log.Println("flushing - block: ", newBlock)

	offset := int16(len(builder.data))
	firstKey := builder.fistKeys[len(builder.fistKeys)-1]
	newBlockMeta := block.NewBlockMeta(offset, firstKey)

	builder.blockMetas = append(builder.blockMetas, newBlockMeta)
	builder.data = append(builder.data, newBlock.Data()...)

	log.Println("flushing meta - : ", builder.blockMetas)

	builder.blockBuilder.Clear()
}

func (builder *TableBuilder) BuildTable(id int, path string) *Table {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Println("file open")
		return nil
	}
	defer file.Close()

	var buffer []byte = builder.data

	for _, meta := range builder.blockMetas {
		encodedMeta := meta.Encode()
		buffer = append(buffer, encodedMeta...)
	}

	offset := len(builder.data)

	offsetByte := make([]byte, reflect.TypeOf(offset).Size())
	binary.LittleEndian.PutUint32(offsetByte, uint32(offset))

	buffer = append(buffer, offsetByte...)

	n, err := file.Write(buffer)
	if err != nil || n != len(buffer) {
		log.Println("write error")
		return nil
	}

	log.Println("buffer : ", buffer)

	return &Table{
		id:               id,
		path:             path,
		file:             file,
		blockMetas:       builder.blockMetas,
		blockMetasOffset: offset,
	}
}
