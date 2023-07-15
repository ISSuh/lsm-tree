package table

import (
	"encoding/binary"
	"os"

	"github.com/ISSuh/lsm-tree/block"
	"github.com/ISSuh/lsm-tree/logging"
)

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

// add key, value at BlockBuilder
// flushing block when data size over than max blcok size
func (builder *TableBuilder) Add(key, value []byte) {
	if builder.blockBuilder.Empty() {
		builder.fistKeys = append(builder.fistKeys, key)
	}

	if builder.blockBuilder.Add(key, value) {
		return
	}

	// flushing block when data size over than max blcok size
	builder.flushingBlock()

	// retry Add to new BlockBuilder
	builder.Add(key, value)
}

func (builder *TableBuilder) flushingBlock() {
	newBlock := builder.blockBuilder.BuildBlock()

	offset := int16(len(builder.data))
	firstKey := builder.fistKeys[len(builder.fistKeys)-1]
	newBlockMeta := block.NewBlockMeta(offset, firstKey)

	builder.blockMetas = append(builder.blockMetas, newBlockMeta)
	builder.data = append(builder.data, newBlock.Encode()...)

	// remvoe old and create new BlockBuilder
	builder.blockBuilder = block.NewBlockBuilder(builder.maxBlockSize)
}

// encode current Blocks, BlcokMetas
func (builder *TableBuilder) encode() []byte {
	buffer := builder.data

	for _, meta := range builder.blockMetas {
		encodedMeta := meta.Encode()
		buffer = append(buffer, encodedMeta...)
	}

	offset := len(builder.data)
	offsetByte := make([]byte, 4)
	binary.LittleEndian.PutUint32(offsetByte, uint32(offset))

	buffer = append(buffer, offsetByte...)

	return buffer
}

func (builder *TableBuilder) BuildTable(id int, path string) *Table {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
	if err != nil {
		logging.Error("BuildTable - file open fail. ", path)
		return nil
	}
	defer file.Close()

	buffer := builder.encode()
	n, err := file.Write(buffer)
	if err != nil || n != len(buffer) {
		logging.Error("BuildTable - write error. erro : ", err, " / size n : ", n)
		return nil
	}

	return &Table{
		id:               id,
		path:             path,
		file:             file,
		blockMetas:       builder.blockMetas,
		blockMetasOffset: len(builder.data),
	}
}