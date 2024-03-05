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

import "encoding/binary"

const (
	LengthTypeSize = 2
)

type Entry struct {
	key   string
	value []byte
}

type Comparetor interface {
	Compare(interface{}, interface{}) bool
}

// entry key
func (entry *Entry) Key() string {
	return entry.key
}

// entry value
func (entry *Entry) Value() []byte {
	return entry.value
}

// encode key, value to
// | keyLengh(int16) | Key([]byte)	|	ValueLengh(int16)	|	value([]byte) |
func (entry *Entry) Encode() []byte {
	var buffer []byte
	lengthByte := make([]byte, LengthTypeSize)

	binary.LittleEndian.PutUint16(lengthByte, uint16(len(entry.key)))
	buffer = append(buffer, lengthByte...)
	buffer = append(buffer, []byte(entry.key)...)

	binary.LittleEndian.PutUint16(lengthByte, uint16(len(entry.value)))
	buffer = append(buffer, lengthByte...)
	buffer = append(buffer, entry.value...)

	return buffer
}

// decode entry from []byte
func (entry *Entry) Decode(data []byte) {
	begin, end := 0, LengthTypeSize
	keyLen := int16(binary.LittleEndian.Uint16(data[begin:end]))

	begin = end
	end += int(keyLen)
	entry.key = string(data[begin:end])

	begin = end
	end += LengthTypeSize
	valueLen := int16(binary.LittleEndian.Uint16(data[begin:end]))

	begin = end
	end += int(valueLen)
	entry.value = data[begin:end]
}
