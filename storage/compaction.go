package storage

import (
	"strconv"
	"sync"

	"github.com/ISSuh/lsm-tree/logging"
	"github.com/ISSuh/lsm-tree/skiplist"
	"github.com/ISSuh/lsm-tree/table"
)

func (storage *Storage) compactOnLevel(level int, fileNames []string) {
	tempMemTable := skiplist.New(storage.option.LevelOnSkipList)

	filePathPrefix := storage.option.Path + "/" + strconv.Itoa(level) + "/"
	for _, fileName := range fileNames {
		filePath := filePathPrefix + fileName
		table := table.OpenTable(0, filePath)
		if table == nil {
			return
		}

		for i := 0; i < table.BlockNum(); i++ {
			block := table.LoadBlock(i)
			iter := block.Iterator()
			for iter != nil {
				tempMemTable.Set(iter.Key(), iter.Value()) //
				iter = iter.Next()
			}
		}
	}

	storage.buildTable(tempMemTable, level)
}

func (storage *Storage) concurrentMerge(lhs, rhs string, tempMemTable *skiplist.SkipList) {
	if (lhs == "") || (rhs == "") {
		logging.Error("Merge - invalid file name. lhs : ", lhs, " / rhs : ", rhs)
		return
	}

	if lhs == rhs {
		logging.Warning("Merge - same file name.")
		return
	}

	var wg sync.WaitGroup
	wg.Add(2)
	pushEntryToTemporaryMemTable := func(filePath string, tempMemTable *skiplist.SkipList) {
		table := table.OpenTable(0, filePath)
		if table == nil {
			wg.Done()
			return
		}

		for i := 0; i < table.BlockNum(); i++ {
			block := table.LoadBlock(i)
			iter := block.Iterator()
			for iter != nil {
				tempMemTable.Set(iter.Key(), iter.Value()) //
				iter = iter.Next()
			}
		}
		wg.Done()
	}

	go pushEntryToTemporaryMemTable(lhs, tempMemTable)
	go pushEntryToTemporaryMemTable(rhs, tempMemTable)
	wg.Wait()

	storage.buildTable(tempMemTable, 0)
}

func (storage *Storage) buildTable(tempMemTable *skiplist.SkipList, level int) {
	nextLevel := level + 1
	tableBuilder := table.NewTableBuilder(storage.option.BlockSize, storage.option.TableSize)
	filePathPrefix := storage.option.Path + "/"
	nextLevelTableSize := storage.option.TableSize * (nextLevel * storage.option.TableSizeOffset)

	node := tempMemTable.Front()
	for node != nil {
		logging.Error("buildTable - ", storage.tableBuilder.Size(), " / ", nextLevelTableSize)
		if tableBuilder.Size() >= nextLevelTableSize {
			// logging.Error("buildTable - ", storage.tableBuilder.Size(), " / ", nextLevelTableSize)

			targetLevel := level
			need, _ := storage.needCompaction(level)
			if need {
				targetLevel = nextLevel
			}

			filePathOnLevelPrefix := filePathPrefix + strconv.Itoa(targetLevel) + "/"
			storage.writeToFile(tableBuilder, nextLevelTableSize, filePathOnLevelPrefix)
		}

		tableBuilder.Add([]byte(node.Key()), node.Value())
		node = node.Next()
	}

	// wrtie remained data to file
	filePathOnLevelPrefix := filePathPrefix + strconv.Itoa(level) + "/"
	storage.writeToFile(tableBuilder, nextLevelTableSize, filePathOnLevelPrefix)
}
