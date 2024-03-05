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

type BlockBuilder struct {
	data         []byte
	offsets      []int16
	maxBlockSize int
	entryNum     uint16
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
func (builder *BlockBuilder) EstimateEncodedSize() int {
	dataByteSize := len(builder.data)
	offsetByteSize := len(builder.offsets) * LengthTypeSize
	return dataByteSize + offsetByteSize + LengthTypeSize
}

func (builder *BlockBuilder) MaxBlockSize() int {
	return builder.maxBlockSize
}

func (builder *BlockBuilder) Add(key, value []byte) bool {
	calculatedBlockSize := builder.EstimateEncodedSize() + len(key) + len(value) + (LengthTypeSize * 2)
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
