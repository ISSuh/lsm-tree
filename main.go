package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"strconv"

	skiplist "github.com/ISSuh/skiplist"
)

type Entry struct {
	keyLength   int16
	key         []byte
	valueLength int16
	value       []byte
}

func EncodeToBytes(p string) []byte {
	buf := &bytes.Buffer{}
	err := binary.Write(buf, binary.LittleEndian, p)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("uncompressed size (bytes): ", len(buf.Bytes()))
	return buf.Bytes()
}

func main() {
	fmt.Println("TEST")
	list_odd := skiplist.New(5)
	list_even := skiplist.New(5)

	for i := 0; i < 20; i++ {
		key := strconv.Itoa(i)
		value := strconv.Itoa(i)

		if i%2 == 0 {
			list_even.Set(key, value)
		} else {
			list_odd.Set(key, value)
		}
	}

	file, err := os.Create("test.db")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer file.Close()

	temp := string("test")
	// test := []byte(temp)
	test := EncodeToBytes(temp)
	n, err := file.Write(test)
	if err != nil {
		fmt.Println(err)
		return
	}

	log.Println(test)
	log.Printf("save %d byte.", n)
}
