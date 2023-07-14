package table

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDecode(t *testing.T) {
	builder := NewTableBuilder(24, 100)
	builder.Add([]byte("aaa"), []byte("aaa"))
	builder.Add([]byte("bbb"), []byte("bbb"))
	builder.Add([]byte("ccc"), []byte("ccc"))
	builder.Add([]byte("ddd"), []byte("ddd"))

	path := "./TestDecode.db"
	table := builder.BuildTable(0, path)

	_, err := os.Stat(path)
	if assert.Nil(t, err) && assert.NotNil(t, table) {
	}

	os.Remove(path)
}
