package block

import (
	"encoding/binary"
)

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

func DecodeBlockMetasFromByte(data []byte) []BlockMeta {
	blockMetasByteSize := len(data)
	blockMetas := make([]BlockMeta, 0)
	offset := 0
	endOffset := LengthTypeSize

	for offset < blockMetasByteSize {
		// decode block offset
		metaOffset := int16(binary.LittleEndian.Uint16(data[offset:endOffset]))

		// decode fist key length
		offset = endOffset
		endOffset = offset + LengthTypeSize
		firstKeyLen := int16(binary.LittleEndian.Uint16(data[offset:endOffset]))

		// decode fist key
		offset = endOffset
		endOffset = offset + int(firstKeyLen)
		firstKey := data[offset:endOffset]

		blockMetas = append(blockMetas, BlockMeta{
			metaOffset: metaOffset,
			fistKey:    firstKey,
		})

		// set next offset
		offset = endOffset
		endOffset = offset + LengthTypeSize
	}

	return blockMetas
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
