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

package table

import (
	"encoding/binary"
	"os"

	"github.com/ISSuh/lsm-tree/block"
	"github.com/ISSuh/lsm-tree/logging"
)

type TableBuilder struct {
	blockBuilder *block.BlockBuilder
	data         []byte
	blockMetas   []block.BlockMeta
	fistKeys     [][]byte
	maxBlockSize int
}

func NewTableBuilder(maxBlockSize int) *TableBuilder {
	return &TableBuilder{
		blockBuilder: block.NewBlockBuilder(maxBlockSize),
		data:         make([]byte, 0),
		blockMetas:   make([]block.BlockMeta, 0),
		fistKeys:     make([][]byte, 0),
		maxBlockSize: maxBlockSize,
	}
}

func (builder *TableBuilder) Size() int {
	return len(builder.data)
}

// add key, value at BlockBuilder
// flushing block when data size over than max blcok size
func (builder *TableBuilder) Add(key, value []byte) {
	if builder.blockBuilder.Empty() {
		builder.fistKeys = append(builder.fistKeys, key)
	}

	if builder.blockBuilder.Add(key, value) {
		return
	}

	// flushing block when data size over than max blcok size
	builder.flushingBlock()

	// retry Add to new BlockBuilder
	builder.Add(key, value)
}

func (builder *TableBuilder) flushingBlock() {
	newBlock := builder.blockBuilder.BuildBlock()

	offset := int16(len(builder.data))
	firstKey := builder.fistKeys[len(builder.fistKeys)-1]
	newBlockMeta := block.NewBlockMeta(offset, firstKey)

	builder.blockMetas = append(builder.blockMetas, newBlockMeta)
	builder.data = append(builder.data, newBlock.Encode()...)

	// remvoe old and create new BlockBuilder
	builder.blockBuilder = block.NewBlockBuilder(builder.maxBlockSize)
}

// encode current Blocks, BlcokMetas
func (builder *TableBuilder) encode() []byte {
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
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
	if err != nil {
		logging.Error("BuildTable - file open fail. ", path)
		return nil
	}
	defer file.Close()

	builder.flushingBlock()

	buffer := builder.encode()
	n, err := file.Write(buffer)
	if err != nil || n != len(buffer) {
		logging.Error("BuildTable - write error. erro : ", err, " / size n : ", n)
		return nil
	}

	return &Table{
		id:               id,
		path:             path,
		file:             file,
		fileSize:         int64(n),
		blockMetas:       builder.blockMetas,
		blockMetasOffset: len(builder.data),
	}
}
