package block

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewBlockBuilder(t *testing.T) {
	builder := NewBlockBuilder(10)
	assert.NotEqual(t, builder, (*BlockBuilder)(nil))
}

func TestMaxBlockSizeValue(t *testing.T) {
	maxBlockSizeValue := 10
	builder := NewBlockBuilder(maxBlockSizeValue)
	assert.Equal(t, builder.MaxBlockSize(), maxBlockSizeValue)
}

func TestAdd(t *testing.T) {
	builder := NewBlockBuilder(10)
	assert.NotEqual(t, builder, (*BlockBuilder)(nil))

	builder.Add([]byte("a"), []byte("a"))
	assert.False(t, builder.Empty())

	assert.Equal(t, builder.offsets[0], int16(0))
	assert.Equal(t, builder.EstimateEncodedSize(), (1+1)*2*(1*2)+2)
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
