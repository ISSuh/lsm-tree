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

const (
	LengthTypeSize = 2
)

type Block struct {
	data     []byte
	offsets  []int16
	entryNum uint16
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

func (block *Block) EntryNum() uint16 {
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
	block.entryNum = binary.LittleEndian.Uint16(data[entryNumValueOffset:])

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
