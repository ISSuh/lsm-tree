package block

import "encoding/binary"

type Entry struct {
	key   []byte
	value []byte
}

func (entry *Entry) Key() []byte {
	return entry.key
}

func (entry *Entry) Value() []byte {
	return entry.value
}

func (entry *Entry) Encode() []byte {
	var buffer []byte
	lengthByte := make([]byte, LengthTypeSize)

	binary.LittleEndian.PutUint16(lengthByte, uint16(len(entry.key)))
	buffer = append(buffer, lengthByte...)
	buffer = append(buffer, entry.key...)

	binary.LittleEndian.PutUint16(lengthByte, uint16(len(entry.value)))
	buffer = append(buffer, lengthByte...)
	buffer = append(buffer, entry.value...)

	return buffer
}

func (entry *Entry) Decode(data []byte) {
	begin, end := 0, LengthTypeSize
	keyLen := int16(binary.LittleEndian.Uint16(data[begin:end]))

	begin = end
	end += int(keyLen)
	entry.key = data[begin:end]

	begin = end
	end += LengthTypeSize
	valueLen := int16(binary.LittleEndian.Uint16(data[begin:end]))

	begin = end
	end += int(valueLen)
	entry.value = data[begin:end]
}
