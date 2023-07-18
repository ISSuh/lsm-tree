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
