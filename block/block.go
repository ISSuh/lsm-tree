package block

import (
	"encoding/binary"
)

const (
	LengthTypeSize = 2
)

type Block struct {
	data     []byte
	offsets  []int16
	entryNum int
}

func (block *Block) Iterator() *Iterator {
	return newBlockIterator(block)
}

func (block *Block) Data() []byte {
	return block.data
}

func (block *Block) Offset() []int16 {
	return block.offsets
}

func (block *Block) EntryNum() int {
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
	block.entryNum = int(binary.LittleEndian.Uint16(data[entryNumValueOffset:]))

	offset := 0
	calculrateOffset := entryNumValueOffset - (int(block.entryNum) * LengthTypeSize)

	block.data = data[offset:calculrateOffset]
	offset += calculrateOffset

	for offset < entryNumValueOffset {
		value := int16(binary.LittleEndian.Uint16(data[offset : offset+LengthTypeSize]))
		block.offsets = append(block.offsets, value)
		offset += LengthTypeSize
	}
}
