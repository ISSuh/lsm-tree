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

package storage

import (
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/ISSuh/lsm-tree/logging"
	"github.com/ISSuh/lsm-tree/table"
	"github.com/stretchr/testify/assert"
)

const (
	DbPath = "./test"
)

func GenRadomValue(min int, max int) int {
	return min + rand.Intn(max-min)
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func ClearDbDir() {
	_, err := os.Stat(DbPath)
	if err == nil {
		os.RemoveAll(DbPath)
	}
}

func ClearFiles(files []string) {
	for _, file := range files {
		_, err := os.Stat(file)
		if err == nil {
			os.Remove(file)
		}
	}
}

func TestMerge(t *testing.T) {
	ClearDbDir()

	// storage := NewStorage(NewOption())

	ClearDbDir()
}

func TestSet(t *testing.T) {
	ClearDbDir()

	option := NewOption()
	option.Path = DbPath
	option.BlockSize = 15 * B
	option.TableSize = 30 * B
	option.MemTableSize = 100 * B
	option.LimitedFilesNumOnL0 = 4

	logging.Error(option)
	storage := NewStorage(option)

	for i := 0; i <= 100; i++ {
		random := i
		keyAndValue := strconv.Itoa(random)

		storage.Set(keyAndValue, []byte(keyAndValue))
	}

	logging.Error("run Get")

	for i := 0; i <= 100; i++ {
		random := i
		keyAndValue := strconv.Itoa(random)

		data := storage.Get(keyAndValue)
		if assert.NotNil(t, data) {
			assert.Equal(t, string(data), keyAndValue)
		}
	}

	storage.Stop()
	ClearDbDir()
}

func TestBackgroundCompaction(t *testing.T) {
	ClearDbDir()

	option := NewOption()
	option.Path = DbPath
	option.BlockSize = 15 * B
	option.TableSize = 30 * B
	option.MemTableSize = 100 * B
	option.LimitedFilesNumOnL0 = 1

	logging.Error(option)
	storage := NewStorage(option)

	tableBuilder := table.NewTableBuilder(option.BlockSize)
	tableBuilder.Add([]byte("0"), []byte("0"))
	tableBuilder.Add([]byte("1"), []byte("1"))
	tableBuilder.Add([]byte("2"), []byte("2"))
	table1 := tableBuilder.BuildTable(0, DbPath+"/1/0.db")
	assert.NotEqual(t, table1, (*table.Table)(nil))

	tableBuilder = table.NewTableBuilder(option.BlockSize)
	tableBuilder.Add([]byte("3"), []byte("3"))
	tableBuilder.Add([]byte("4"), []byte("4"))
	tableBuilder.Add([]byte("5"), []byte("5"))
	table2 := tableBuilder.BuildTable(1, DbPath+"/1/1.db")
	assert.NotEqual(t, table2, (*table.Table)(nil))

	tableBuilder = table.NewTableBuilder(option.BlockSize)
	tableBuilder.Add([]byte("6"), []byte("6"))
	tableBuilder.Add([]byte("7"), []byte("7"))
	tableBuilder.Add([]byte("8"), []byte("8"))
	table3 := tableBuilder.BuildTable(2, DbPath+"/1/2.db")
	assert.NotEqual(t, table3, (*table.Table)(nil))

	storage.tableId[1] = 3

	storage.tables[1] = append(storage.tables[1], table1, table2, table3)

	for i := 1; i <= 7; i++ {
		key := strconv.Itoa(i)
		value := strconv.Itoa(i + 1)
		storage.Set(key, []byte(value))
	}

	storage.flushMemtableSignal <- true
	<-storage.switchTable

	time.Sleep(1000 * time.Millisecond)

	key := strconv.Itoa(1)
	value := storage.Get(key)
	assert.Equal(t, string(value), "2")

	ClearDbDir()
}
