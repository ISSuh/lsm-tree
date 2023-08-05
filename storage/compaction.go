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
	"github.com/ISSuh/lsm-tree/logging"
	"github.com/ISSuh/lsm-tree/table"
	"github.com/ISSuh/lsm-tree/util"
	"github.com/ISSuh/skiplist"
)

func (storage *Storage) compact(level int) {
	if level < 0 {
		return
	}

	nextLevel := level + 1

	var newTables []*table.Table
	if level == 0 {
		newTables = storage.tableListOnLevel(level)
	} else {
		needMergeTableNum := storage.calculateCompactionTableNum(level)
		if needMergeTableNum <= 0 {
			return
		}

		newTables = storage.choiceWillMergedTable(level, needMergeTableNum)
	}

	// load newer table to memory
	newMemTable := storage.loadTableToHeap(level, newTables)
	if newMemTable == nil {
		logging.Error("compactOnLevel0 - can not load new table. ")
		return
	}

	var targetTables []*table.Table
	var baseMemTable *skiplist.SkipList
	if len(storage.tables[nextLevel]) > 0 {
		firstKey := newMemTable.Front().Key()
		lastKey := newMemTable.Back().Key()

		// find target table about under compacttion range at next level
		targetTables = storage.findCompactionTargetOnNextLevel(nextLevel, firstKey, lastKey)
		baseMemTable = storage.loadTableToHeap(nextLevel, targetTables)
		if baseMemTable == nil {
			logging.Error("compactOnLevel0 - can not load old table. ")
			return
		}

		// merge two table
		storage.mergeTable(newMemTable, baseMemTable)
	} else {
		baseMemTable = newMemTable
	}

	// flushing table to disk
	storage.flushing(nextLevel, baseMemTable)

	// erase already merged table
	{
		storage.tableMutex.Lock()

		storage.eraseOldTable(level, newTables)
		if len(targetTables) > 0 {
			storage.eraseOldTable(nextLevel, targetTables)
		}

		util.RemoveTableFile(newTables)
		if len(targetTables) > 0 {
			util.RemoveTableFile(targetTables)
		}

		storage.tableMutex.Unlock()
	}
}

// merge two mem table.
func (storage *Storage) mergeTable(newTable, baseTable *skiplist.SkipList) {
	node := newTable.Front()
	for node != nil {
		baseTable.Set(node.Key(), node.Value())
		node = node.Next()
	}
}

// load table to memory
func (storage *Storage) loadTableToHeap(level int, tables []*table.Table) *skiplist.SkipList {
	if tables == nil {
		return nil
	}

	memTable := skiplist.New(storage.option.LevelOnSkipList)
	for _, targetTable := range tables {
		table := table.OpenTable(level, targetTable.FileName())
		if table == nil {
			return nil
		}

		for i := 0; i < table.BlockNum(); i++ {
			block := table.LoadBlock(i)
			iter := block.Iterator()
			for iter != nil {
				memTable.Set(iter.Key(), iter.Value())
				iter = iter.Next()
			}
		}
	}
	return memTable
}

// find compaction target table on next level
func (storage *Storage) findCompactionTargetOnNextLevel(nextLevel int, begin, end string) []*table.Table {
	beginIndex, endIndex := 0, 0
	for i, table := range storage.tables[nextLevel] {
		firstKey := table.FirstKey()
		if begin >= firstKey {
			beginIndex = i
			endIndex = i
		}

		if (begin <= end) && (end >= firstKey) {
			endIndex = i
		}
	}

	logging.Error("findCompactionTargetOnNextLevel - [", begin, " / ", end, "], [", beginIndex, " / ", endIndex, "]")

	var targetTable []*table.Table
	tables := storage.tables[nextLevel][beginIndex : endIndex+1]
	for _, table := range tables {
		targetTable = append(targetTable, table)
	}
	return targetTable
}

func (storage *Storage) calculateCompactionTableNum(level int) int {
	needMergeTableNum := 0
	limitedFileSizeOnLevel := (storage.option.LimitedFilesNumOnL0 * storage.option.TableSize) * (level * storage.option.TableSizeOffset)
	totalFileSize := util.TotalTableSizeOnLevel(storage.option.Path, level)
	if int64(limitedFileSizeOnLevel) >= totalFileSize {
		needMergeTableNum = int((int64(limitedFileSizeOnLevel) - totalFileSize) / int64(storage.option.TableSize))
	}
	return needMergeTableNum
}

func (storage *Storage) choiceWillMergedTable(level, needMergeTableNum int) []*table.Table {
	var tables []*table.Table
	for i := 0; i < int(needMergeTableNum); i++ {
		tables = append(tables, storage.tables[level][i])
	}
	return tables
}

func (storage *Storage) tableListOnLevel(level int) []*table.Table {
	var tables []*table.Table
	for _, table := range storage.tables[0] {
		tables = append(tables, table)
	}
	return tables
}
