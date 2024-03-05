/*
MIT License

Copyright (c) 2023 ISSuh

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

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
