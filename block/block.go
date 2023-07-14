package block

import (
	"encoding/binary"
	"log"
)

const (
	LengthTypeSize = 2
)

type Block struct {
	data    []byte
	offsets []int16
}

func (block *Block) Data() []byte {
	return block.data
}

func (block *Block) Offset() []int16 {
	return block.offsets
}

type BlockMeta struct {
	offset  int16
	fistKey []byte
}

func NewBlockMeta(offset int16, fistKey []byte) BlockMeta {
	return BlockMeta{
		offset:  offset,
		fistKey: fistKey,
	}
}

func (meta *BlockMeta) Offset() int16 {
	return meta.offset
}

func (meta *BlockMeta) FirstKey() []byte {
	return meta.fistKey
}

func (meta *BlockMeta) Encode() []byte {
	var buffer []byte
	offsetByte := make([]byte, LengthTypeSize)

	binary.LittleEndian.PutUint16(offsetByte, uint16(meta.offset))
	buffer = append(buffer, offsetByte...)
	buffer = append(buffer, meta.fistKey...)
	return buffer
}

func (meta *BlockMeta) Decode(data []byte) {
	meta.offset = int16(binary.LittleEndian.Uint16(data[0:LengthTypeSize]))
	meta.fistKey = data[LengthTypeSize:]
}

type BlockBuilder struct {
	data         []byte
	offsets      []int16
	maxBlockSize int
}

func NewBlockBuilder(maxBlockSize int) *BlockBuilder {
	return &BlockBuilder{
		data:         make([]byte, 0),
		offsets:      make([]int16, 0),
		maxBlockSize: maxBlockSize,
	}
}

func (builder *BlockBuilder) Empty() bool {
	return len(builder.offsets) == 0
}

func (builder *BlockBuilder) Clear() {
	builder.data = make([]byte, 0)
	builder.offsets = make([]int16, 0)
}

func (builder *BlockBuilder) Size() int {
	offsetByteSize := len(builder.offsets) * LengthTypeSize
	dataByteSize := len(builder.data)
	// entryCountByteSize := LengthTypeSize

	return offsetByteSize + dataByteSize
}

func (builder *BlockBuilder) MaxBlockSize() int {
	return builder.maxBlockSize
}

func (builder *BlockBuilder) Add(key, value []byte) bool {
	calculatedBlockSize := builder.Size() + len(key) + len(value) + (LengthTypeSize * 2)
	log.Println("blockBuilder::Add - calculatedBlockSize: ", calculatedBlockSize)

	if !builder.Empty() && calculatedBlockSize > builder.MaxBlockSize() {
		return false
	}

	var buffer []byte
	lengthByte := make([]byte, LengthTypeSize)

	binary.LittleEndian.PutUint16(lengthByte, uint16(len(key)))
	buffer = append(buffer, lengthByte...)
	buffer = append(buffer, key...)

	binary.LittleEndian.PutUint16(lengthByte, uint16(len(value)))
	buffer = append(buffer, lengthByte...)
	buffer = append(buffer, value...)

	builder.offsets = append(builder.offsets, int16(len(builder.data)))
	builder.data = append(builder.data, buffer...)
	return true
}

func (builder *BlockBuilder) BuildBlock() *Block {
	copidData := make([]byte, len(builder.data))
	copy(copidData, builder.data)

	copidOffsets := make([]int16, len(builder.offsets))
	copy(copidOffsets, builder.offsets)

	return &Block{
		data:    copidData,
		offsets: copidOffsets,
	}
}
