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

package entry

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
)

func EncodeTestKeyValue(key string, value []byte) []byte {
	var buffer []byte
	lengthByte := make([]byte, LengthTypeSize)

	binary.LittleEndian.PutUint16(lengthByte, uint16(len(key)))
	buffer = append(buffer, lengthByte...)
	buffer = append(buffer, []byte(key)...)

	binary.LittleEndian.PutUint16(lengthByte, uint16(len(value)))
	buffer = append(buffer, lengthByte...)
	buffer = append(buffer, value...)

	return buffer
}

func TestEntrySetAndGetEntry(t *testing.T) {
	entry := Entry{
		key:   "test",
		value: []byte("test"),
	}

	assert.Equal(t, entry.Key(), "test")
	assert.Equal(t, entry.Value(), []byte("test"))
}

func TestEntryEncode(t *testing.T) {
	result := EncodeTestKeyValue("a", []byte("a"))

	entry := Entry{
		key:   "a",
		value: []byte("a"),
	}

	encodedEntry := entry.Encode()

	assert.Equal(t, len(encodedEntry), 6)
	assert.Equal(t, encodedEntry, result)
}

func TestEntryDecode(t *testing.T) {
	resultEntry := Entry{
		key:   "a",
		value: []byte("a"),
	}

	encodedEntry := EncodeTestKeyValue("a", []byte("a"))

	var entry Entry
	entry.Decode(encodedEntry)

	assert.Equal(t, entry, resultEntry)
}
