package table

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDecode(t *testing.T) {
	path := "./TestDecode.db"
	_, err := os.Stat(path)
	if err == nil {
		os.Remove(path)
	}

	builder := NewTableBuilder(12, 100)
	builder.Add([]byte("aaa"), []byte("aaa"))
	builder.Add([]byte("bbb"), []byte("bbb"))
	builder.Add([]byte("ccc"), []byte("ccc"))
	builder.Add([]byte("ddd"), []byte("ddd"))
	builder.Add([]byte("eeeee"), []byte("eeeee"))
	builder.Add([]byte("fff"), []byte("fff"))
	builder.Add([]byte("ddd"), []byte("ddd"))

	table := builder.BuildTable(0, path)

	_, err = os.Stat(path)
	if assert.Nil(t, err) && assert.NotNil(t, table) {
		newTable := OpenTable(0, path)

		buffer := builder.blockMetas
		tempBuffer := newTable.blockMetas

		assert.Equal(t, buffer, tempBuffer)
	}

	os.Remove(path)
}

func TestLoadBlock(t *testing.T) {
	path := "./TestDecode.db"
	_, err := os.Stat(path)
	if err == nil {
		os.Remove(path)
	}

	builder := NewTableBuilder(10, 100)
	builder.Add([]byte("aaa"), []byte("aaa"))
	builder.Add([]byte("bbb"), []byte("bbb"))
	builder.Add([]byte("ccc"), []byte("ccc"))
	builder.Add([]byte("ddd"), []byte("ddd"))

	table := builder.BuildTable(0, path)

	_, err = os.Stat(path)
	if assert.Nil(t, err) && assert.NotNil(t, table) {
		block := table.LoadBlock(2)
		if assert.NotNil(t, block) {
			iter := block.Iterator()
			if assert.NotNil(t, iter) {
				assert.Equal(t, iter.Key(), "ccc")
				assert.Equal(t, iter.Value(), []byte("ccc"))
			}
		}
	}

	os.Remove(path)
}
