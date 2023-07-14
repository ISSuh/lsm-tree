package block

import "encoding/binary"

const (
	lengthTypeSize = 2
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
	offsetByte := make([]byte, lengthTypeSize)

	binary.LittleEndian.PutUint16(offsetByte, uint16(meta.offset))
	buffer = append(buffer, offsetByte...)
	buffer = append(buffer, meta.fistKey...)
	return buffer
}

func (meta *BlockMeta) Decode() BlockMeta {

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
	offsetByteSize := len(builder.offsets) * lengthTypeSize
	dataByteSize := len(builder.data)
	entryCountByteSize := lengthTypeSize

	return offsetByteSize + dataByteSize + entryCountByteSize
}

func (builder *BlockBuilder) MaxBlockSize() int {
	return builder.maxBlockSize
}

func (builder *BlockBuilder) Add(key, value []byte) bool {
	calculatedBlockSize := builder.Size() + len(key) + len(value) + (lengthTypeSize * 3)
	if !builder.Empty() && calculatedBlockSize > builder.MaxBlockSize() {
		return false
	}

	builder.offsets = append(builder.offsets, int16(len(builder.data)))

	var buffer []byte
	lengthByte := make([]byte, 2)

	binary.LittleEndian.PutUint16(lengthByte, uint16(len(key)))
	buffer = append(buffer, lengthByte...)
	buffer = append(buffer, key...)

	binary.LittleEndian.PutUint16(lengthByte, uint16(len(value)))
	buffer = append(buffer, lengthByte...)
	buffer = append(buffer, value...)
	return true
}

func (builder *BlockBuilder) BuildBlock() *Block {
	return &Block{
		data:    builder.data,
		offsets: builder.offsets,
	}
}
