package block

import (
	"encoding/binary"
	"log"
)

const (
	LengthTypeSize = 2
)

type Block struct {
	data     []byte
	offsets  []int16
	entryNum int16
}

func (block *Block) Data() []byte {
	return block.data
}

func (block *Block) Offset() []int16 {
	return block.offsets
}

func (block *Block) EntryNum() int16 {
	return block.entryNum
}

func (block *Block) Encode() []byte {
	buffer := block.data
	for _, offset := range block.offsets {
		offsetByte := make([]byte, LengthTypeSize)
		binary.LittleEndian.PutUint16(offsetByte, uint16(offset))
		buffer = append(buffer, offsetByte...)
	}

	entryNumByte := make([]byte, LengthTypeSize)
	binary.LittleEndian.PutUint16(entryNumByte, uint16(block.entryNum))
	buffer = append(buffer, entryNumByte...)

	return buffer
}

func (block *Block) Decode(data []byte) {
	entryNumValueOffset := len(data) - LengthTypeSize
	block.entryNum = int16(binary.LittleEndian.Uint16(data[entryNumValueOffset:]))

	offset := 0
	calculrateOffset := entryNumValueOffset - (int(block.entryNum) * LengthTypeSize)

	block.data = data[offset:calculrateOffset]
	offset += calculrateOffset

	for offset < entryNumValueOffset {
		value := int16(binary.LittleEndian.Uint16(data[offset:LengthTypeSize]))
		block.offsets = append(block.offsets, value)
		offset += LengthTypeSize
	}
}

type BlockMeta struct {
	metaOffset int16
	fistKey    []byte
}

func NewBlockMeta(offset int16, fistKey []byte) BlockMeta {
	return BlockMeta{
		metaOffset: offset,
		fistKey:    fistKey,
	}
}

func (meta *BlockMeta) Offset() int16 {
	return meta.metaOffset
}

func (meta *BlockMeta) FirstKey() []byte {
	return meta.fistKey
}

func (meta *BlockMeta) Encode() []byte {
	var buffer []byte
	offsetByte := make([]byte, LengthTypeSize)
	binary.LittleEndian.PutUint16(offsetByte, uint16(meta.metaOffset))

	fistKeyLengthByte := make([]byte, LengthTypeSize)
	binary.LittleEndian.PutUint16(fistKeyLengthByte, uint16(len(meta.fistKey)))

	buffer = append(buffer, offsetByte...)
	buffer = append(buffer, fistKeyLengthByte...)
	buffer = append(buffer, meta.fistKey...)
	return buffer
}

func (meta *BlockMeta) Decode(data []byte) {
	offset := 0

	meta.metaOffset = int16(binary.LittleEndian.Uint16(data[offset:LengthTypeSize]))
	offset += LengthTypeSize

	// meta.metaOffset = int16(binary.LittleEndian.Uint16(data[offset:LengthTypeSize]))
	// offset += LengthTypeSize

	meta.fistKey = data[offset:]
}

type BlockBuilder struct {
	data         []byte
	offsets      []int16
	maxBlockSize int
	entryNum     int16
}

func NewBlockBuilder(maxBlockSize int) *BlockBuilder {
	return &BlockBuilder{
		data:         make([]byte, 0),
		offsets:      make([]int16, 0),
		maxBlockSize: maxBlockSize,
		entryNum:     0,
	}
}

func (builder *BlockBuilder) Empty() bool {
	return len(builder.offsets) == 0
}

// blcok size is (entries byte buffer) * (sizeof(int16) * number of offset) * (sizeof(int16))
func (builder *BlockBuilder) Size() int {
	dataByteSize := len(builder.data)
	offsetByteSize := len(builder.offsets) * LengthTypeSize
	return dataByteSize + offsetByteSize + LengthTypeSize
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

	builder.entryNum++
	return true
}

func (builder *BlockBuilder) BuildBlock() *Block {
	return &Block{
		data:     builder.data,
		offsets:  builder.offsets,
		entryNum: builder.entryNum,
	}
}
