package lsmtree

const (
	lengthTypeSize = 2
)

type Block struct {
	date    []byte
	offsets []int16
}

type BlockBuilder struct {
	date         []byte
	offsets      []int16
	maxBlockSize int
}

func (builder *BlockBuilder) empty() bool {
	return len(builder.offsets) == 0
}

func (builder *BlockBuilder) size() int {
	offsetByteSize := len(builder.offsets) * lengthTypeSize
	dataByteSize := len(builder.date)
	entryCountByteSize := lengthTypeSize

	return offsetByteSize + dataByteSize + entryCountByteSize
}

func (builder *BlockBuilder) limitedBlockSize() int {
	return builder.maxBlockSize
}

func (builder *BlockBuilder) addItem(key, value []byte) bool {
	calculatedBlockSize := builder.size() + len(key) + len(value) + (lengthTypeSize * 3)
	if !builder.empty() && calculatedBlockSize > builder.limitedBlockSize() {
		return false
	}

	return true
}

func (builder *BlockBuilder) buildBlock() *Block {
	return &Block{
		date:    builder.date,
		offsets: builder.offsets,
	}
}
