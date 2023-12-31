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
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

var testEncodedBlock []byte = []byte{1, 0, 97, 1, 0, 97, 0, 0, 1, 0}

func TestNewBlockBuilder(t *testing.T) {
	builder := NewBlockBuilder(10)
	assert.NotEqual(t, builder, (*BlockBuilder)(nil))
}

func TestMaxBlockSizeValue(t *testing.T) {
	maxBlockSizeValue := 10
	builder := NewBlockBuilder(maxBlockSizeValue)
	assert.Equal(t, builder.MaxBlockSize(), maxBlockSizeValue)
}

func TestAdd1(t *testing.T) {
	builder := NewBlockBuilder(10)
	assert.NotEqual(t, builder, (*BlockBuilder)(nil))

	assert.True(t, builder.Add([]byte("a"), []byte("a")))
	assert.False(t, builder.Empty())

	assert.Equal(t, builder.offsets[0], int16(0))
	assert.Equal(t, builder.EstimateEncodedSize(), (1+1)*2*(1*2)+2)
}

func TestAdd2(t *testing.T) {
	builder := NewBlockBuilder(10)
	assert.NotEqual(t, builder, (*BlockBuilder)(nil))

	assert.True(t, builder.Add([]byte("a"), []byte("a")))
	assert.False(t, builder.Add([]byte("b"), []byte("b")))
	assert.False(t, builder.Empty())

	assert.Equal(t, builder.offsets[0], int16(0))
	assert.Equal(t, builder.EstimateEncodedSize(), (1+1)*2*(1*2)+2)
}

func TestBuildBlock(t *testing.T) {
	builder := NewBlockBuilder(20)
	assert.NotEqual(t, builder, (*BlockBuilder)(nil))

	for i := 0; i < 2; i++ {
		builder.Add([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i)))
	}
	assert.False(t, builder.Empty())

	block := builder.BuildBlock()
	assert.NotEqual(t, block, (*Block)(nil))
	assert.NotEqual(t, block.Data(), ([]byte)(nil))

	assert.Equal(t, block.EntryNum(), 2)
	assert.Equal(t, block.Offset(), []int16{0, 6})
}

func TestBlockEncode(t *testing.T) {
	builder := NewBlockBuilder(10)
	assert.NotEqual(t, builder, (*BlockBuilder)(nil))

	builder.Add([]byte("a"), []byte("a"))

	block := builder.BuildBlock()
	assert.NotEqual(t, block, (*Block)(nil))
	assert.NotEqual(t, block.Data(), ([]byte)(nil))

	assert.Equal(t, block.Encode(), testEncodedBlock)
}

func TestBlockDecode(t *testing.T) {
	var block Block
	block.Decode(testEncodedBlock)

	assert.NotEqual(t, block.data, ([]byte)(nil))
	assert.Equal(t, block.Data(), []byte{1, 0, 97, 1, 0, 97})
	assert.Equal(t, block.EntryNum(), 1)
	assert.Equal(t, block.Offset(), []int16{0})
}

func TestInvalidIterator(t *testing.T) {
	iter := newBlockIterator(nil)
	assert.Equal(t, iter, (*Iterator)(nil))
}

func TestIteratorNext(t *testing.T) {
	builder := NewBlockBuilder(100)
	assert.NotEqual(t, builder, (*BlockBuilder)(nil))

	for i := 0; i < 2; i++ {
		builder.Add([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i)))
	}
	assert.False(t, builder.Empty())

	block := builder.BuildBlock()
	assert.NotEqual(t, block, (*Block)(nil))

	iter := block.Iterator()
	assert.NotEqual(t, iter, (*Iterator)(nil))

	item := 0
	for iter != nil {
		key := iter.Key()
		value := iter.Value()

		assert.Equal(t, key, strconv.Itoa(item))
		assert.Equal(t, value, []byte(strconv.Itoa(item)))

		iter = iter.Next()
		item++
	}
}
