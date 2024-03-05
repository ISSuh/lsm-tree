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

	builder := NewTableBuilder(12)
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

	builder := NewTableBuilder(10)
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
