// package main

// import (
// 	"encoding/binary"
// 	"fmt"
// 	"log"
// 	"os"
// 	"strconv"

// 	skiplist "github.com/ISSuh/skiplist"
// )

// type Entry struct {
// 	KeyLength   int16
// 	Key         []byte
// 	ValueLength int16
// 	Value       []byte
// }

// func EncodeToBytes(entry *Entry) []byte {
// 	var buffer []byte

// 	lengthByte := make([]byte, 2)
// 	binary.LittleEndian.PutUint16(lengthByte, uint16(entry.KeyLength))

// 	buffer = append(buffer, lengthByte...)
// 	buffer = append(buffer, entry.Key...)

// 	binary.LittleEndian.PutUint16(lengthByte, uint16(entry.ValueLength))
// 	buffer = append(buffer, lengthByte...)
// 	buffer = append(buffer, entry.Value...)

// 	return buffer
// }

// func DecodeFromBytes(data []byte) *Entry {
// 	entry := &Entry{}
// 	var offset int16 = 0

// 	entry.KeyLength = int16(binary.LittleEndian.Uint16(data[offset:2]))
// 	offset += 2

// 	entry.Key = data[offset : offset+entry.KeyLength]
// 	offset += entry.KeyLength

// 	entry.ValueLength = int16(binary.LittleEndian.Uint16(data[offset : offset+2]))
// 	offset += 2

// 	entry.Value = data[offset:]

// 	return entry
// }

// func main() {
// 	fmt.Println("TEST")
// 	list_odd := skiplist.New(5)
// 	list_even := skiplist.New(5)

// 	for i := 0; i < 20; i++ {
// 		key := strconv.Itoa(i)
// 		value := strconv.Itoa(i)

// 		if i%2 == 0 {
// 			list_even.Set(key, value)
// 		} else {
// 			list_odd.Set(key, value)
// 		}
// 	}

// 	file, err := os.Create("test.db")
// 	if err != nil {
// 		fmt.Println(err)
// 		return
// 	}
// 	defer file.Close()

// 	entry := Entry{
// 		KeyLength:   5,
// 		Key:         []byte("xxxxx"),
// 		ValueLength: 6,
// 		Value:       []byte("aaaaaa"),
// 	}

// 	test := EncodeToBytes(&entry)
// 	n, err := file.Write(test)
// 	if err != nil {
// 		fmt.Println(err)
// 		return
// 	}

// 	fmt.Println(test)

// 	DecodeFromBytes(test)

// 	log.Println(test)
// 	log.Printf("save %d byte.", n)
// }
