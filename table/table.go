package table

import (
	"encoding/binary"
	"log"
	"os"

	block "github.com/ISSuh/lsm-tree/block"
)

const (
	BlockMetaOffetTypeSize = 4
)

type Table struct {
	id               int
	path             string
	file             *os.File
	blockMetas       []block.BlockMeta
	blockMetasOffset int
}

func OpenTable(id int, path string) *Table {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Println("file open fail")
		return nil
	}
	defer file.Close()

	table := &Table{
		id:               id,
		path:             path,
		file:             file,
		blockMetas:       make([]block.BlockMeta, 0),
		blockMetasOffset: 0,
	}

	info, err := table.file.Stat()
	if err != nil {
		log.Println("get file stat fail")
	}

	table.decodeBlockMetaOffset(info.Size())
	table.decodeBlockMetas(info.Size())
	return table
}

func (table *Table) decodeBlockMetaOffset(fileSize int64) {
	blockMetaOffsetByte := make([]byte, BlockMetaOffetTypeSize)
	n, err := table.file.ReadAt(blockMetaOffsetByte, fileSize-BlockMetaOffetTypeSize)
	if (err != nil) || (n != BlockMetaOffetTypeSize) {
		log.Println("read error")
	}

	table.blockMetasOffset = int(binary.LittleEndian.Uint32(blockMetaOffsetByte))
}

func (table *Table) decodeBlockMetas(fileSize int64) {
	calculateblockMetasSize := fileSize - int64(table.blockMetasOffset-BlockMetaOffetTypeSize)

	blockMetasByte := make([]byte, calculateblockMetasSize)
	n, err := table.file.ReadAt(blockMetasByte, int64(table.blockMetasOffset))
	if (err != nil) || (n != int(calculateblockMetasSize)) {
		log.Println("read error")
	}

	// TODO : need decode BlockMetas from byte
	// offset := 0
	// for offset < calculateblockMetasSize {

	// }
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
	builder.data = append(builder.data, newBlock.Encode()...)

	log.Println("flushing - block: ", newBlock)
	log.Println("flushing - block encode : ", newBlock.Encode())
	log.Println("flushing - meta - : ", newBlockMeta)
	log.Println("flushing - meta encode : ", newBlockMeta.Encode())

	builder.blockBuilder = block.NewBlockBuilder(builder.maxBlockSize)
}

func (builder *TableBuilder) serialize() []byte {
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
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Println("file open fail")
		return nil
	}
	defer file.Close()

	buffer := builder.serialize()
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
		blockMetasOffset: len(builder.data),
	}
}
