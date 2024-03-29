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
	"strconv"
	"sync"

	"github.com/ISSuh/lsm-tree/internal/logging"
	"github.com/ISSuh/lsm-tree/internal/table"
	"github.com/ISSuh/lsm-tree/internal/util"
	"github.com/ISSuh/skiplist"
)

type Storage struct {
	option Option

	tableBuilder *table.TableBuilder

	memTable  *skiplist.SkipList
	immuTable *skiplist.SkipList

	memTableMutex sync.RWMutex
	tableMutex    sync.RWMutex

	backgrounCompactSignal chan int
	flushMemtableSignal    chan bool
	switchTable            chan bool
	terminateSync          sync.WaitGroup

	tableId map[int]int
	tables  [][]*table.Table
}

func NewStorage(option Option) *Storage {
	tableIdMap := make(map[int]int)
	for i := 0; i < option.Level; i++ {
		tableIdMap[i] = 0
	}

	tables := make([][]*table.Table, option.Level)
	for i := range tables {
		tables[i] = make([]*table.Table, 0)
	}

	storage := &Storage{
		option:                 option,
		memTable:               skiplist.New(option.LevelOnSkipList),
		immuTable:              nil,
		backgrounCompactSignal: make(chan int, 100),
		flushMemtableSignal:    make(chan bool, 100),
		switchTable:            make(chan bool),
		tableId:                tableIdMap,
		tables:                 tables,
	}

	if !util.CreateLevelDirectory(option.Path, option.Level) {
		return nil
	}

	storage.terminateSync.Add(2)
	go storage.backgroundCompact()
	go storage.flushMemTable()
	return storage
}

func (storage *Storage) Set(key string, value []byte) {
	storage.memTable.Set(key, value)
	if storage.memTable.Size() >= uint64(storage.option.MemTableSize) {
		storage.flushMemtableSignal <- true
		<-storage.switchTable
	}
}

func (storage *Storage) Get(key string) []byte {
	var data []byte = nil

	// find on memtabe
	data = storage.findAtMemTable(key, storage.memTable)
	if data != nil {
		return data
	}

	// find on immutabe
	data = storage.findAtMemTable(key, storage.immuTable)
	if data != nil {
		return data
	}

	// find on tabkes
	return storage.findAtTable(key)
}

func (storage *Storage) Remove(key string) {
}

func (storage *Storage) Stop() {
	storage.flushMemtableSignal <- false
	storage.backgrounCompactSignal <- -1
	storage.terminateSync.Wait()

	// storage.compact(storage.memTable)
}

func (storage *Storage) eraseOldTable(level int, oldTable []*table.Table) {
	for _, targetTable := range oldTable {
		storage.eraseTableById(level, targetTable.Id())
	}
}

func (storage *Storage) eraseTableById(level, tableId int) {
	for i, targetTable := range storage.tables[level] {
		if targetTable.Id() == tableId {
			storage.tables[level] = append(storage.tables[level][:i], storage.tables[level][i+1:]...)
			break
		}
	}
}

func (storage *Storage) flushMemTable() {
	for run := range storage.flushMemtableSignal {
		logging.Info("flushMemTable - flushing")
		level := 0

		{
			storage.memTableMutex.Lock()
			storage.immuTable = storage.memTable
			storage.memTable = skiplist.New(storage.option.LevelOnSkipList)
			storage.memTableMutex.Unlock()
		}

		if run {
			storage.switchTable <- true
		}

		storage.flushing(0, storage.immuTable)

		{
			storage.memTableMutex.Lock()
			storage.immuTable = nil
			storage.memTableMutex.Unlock()
		}

		if len(storage.tables[level]) >= storage.option.LimitedFilesNumOnL0 {
			storage.backgrounCompactSignal <- level
		}

		if !run {
			logging.Info("flushMemTable - signal is false.", run)
			break
		}
	}

	storage.terminateSync.Done()
	logging.Info("flushMemTable - done")
}

func (storage *Storage) backgroundCompact() {
	for level := range storage.backgrounCompactSignal {
		logging.Info("flushMemTable - compaction level ", level)
		storage.compact(level)

		if level < 0 {
			logging.Info("backgroundCompact - signal is false.", level)
			break
		}
	}

	storage.terminateSync.Done()
	logging.Info("backgroundCompact - done")
}

func (storage *Storage) flushing(targetLevel int, memTable *skiplist.SkipList) {
	filePathPrefix := storage.option.Path + "/" + strconv.Itoa(targetLevel) + "/"
	tableBuilder := table.NewTableBuilder(storage.option.BlockSize)

	node := memTable.Front()
	for node != nil {
		if tableBuilder.Size() >= storage.option.TableSize {
			storage.writeToFile(targetLevel, tableBuilder, filePathPrefix)
			tableBuilder = table.NewTableBuilder(storage.option.BlockSize)
		}

		tableBuilder.Add([]byte(node.Key()), node.Value())
		node = node.Next()
	}

	// wrtie remained data to filez
	storage.writeToFile(targetLevel, tableBuilder, filePathPrefix)
}

func (storage *Storage) writeToFile(level int, tableBuilder *table.TableBuilder, filePathPrefix string) {
	storage.tableMutex.Lock()
	defer storage.tableMutex.Unlock()

	file := filePathPrefix + strconv.Itoa(storage.tableId[level]) + ".db"
	newTable := tableBuilder.BuildTable(storage.tableId[level], file)
	storage.tables[level] = append(storage.tables[level], newTable)

	storage.tableId[level]++
}

func (storage *Storage) findAtMemTable(key string, memTable *skiplist.SkipList) []byte {
	storage.memTableMutex.Lock()
	defer storage.memTableMutex.Unlock()

	if memTable == nil {
		return nil
	}

	if memTable != nil {
		data := memTable.Get(key)
		if data != nil {
			return data.Value()
		}
	}
	return nil
}

func (storage *Storage) findAtTable(key string) []byte {
	storage.tableMutex.Lock()
	defer storage.tableMutex.Unlock()

	totalTableNum := 0
	for level := 0; level < storage.option.Level; level++ {
		totalTableNum += len(storage.tables[level])
	}

	type Result struct {
		value   []byte
		tableId int
	}

	resultQueue := make(chan Result, totalTableNum)
	var wg sync.WaitGroup

	wg.Add(totalTableNum)
	for level := 0; level < storage.option.Level; level++ {
		for _, item := range storage.tables[level] {
			go func(table *table.Table) {
				defer wg.Done()
				leader := table.NewTableLeader()
				value := leader.Get(key)

				if value != nil {
					resultQueue <- Result{value: value, tableId: table.Id()}
				}
			}(item)
		}
	}
	wg.Wait()
	close(resultQueue)

	var value []byte = nil
	for item := range resultQueue {
		value = item.value
	}
	return value
}
