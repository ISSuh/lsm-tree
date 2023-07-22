package storage

import (
	"io/ioutil"
	"strconv"
	"sync"

	"github.com/ISSuh/lsm-tree/logging"
	"github.com/ISSuh/lsm-tree/skiplist"
	"github.com/ISSuh/lsm-tree/table"
)

func (storage *Storage) needCompaction(level int) (bool, []string) {
	levelDirPath := storage.option.Path + "/" + strconv.Itoa(level)
	files, err := ioutil.ReadDir(levelDirPath)
	if err != nil {
		logging.Error("checkNeedCompaction - can not read dir", levelDirPath, ", err : ", err)
		return false, nil
	}

	if len(files) < storage.option.LimitedFilesNum[level] {
		return false, nil
	}

	var fileNames []string
	for _, file := range files {
		fileNames = append(fileNames, file.Name())
	}
	return true, fileNames
}

func (storage *Storage) compact() {
	for level := 0; level < storage.option.Level; level++ {
		need, fileNames := storage.needCompaction(level)
		if need {
			logging.Error("backgroundWork - will compact level ", level, " / ", fileNames)
			storage.compactOnLevel(level, fileNames)
			// util.RemoveMergedFile(storage.option.Path, level, fileNames)
		}
	}
}

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
				tempMemTable.Set(iter.Key(), iter.Value())
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
	nextLevelTableSize := storage.option.TableSize * (nextLevel * storage.option.TableSizeOffset)
	tableBuilder := table.NewTableBuilder(storage.option.BlockSize, nextLevelTableSize)
	filePathPrefix := storage.option.Path + "/"

	node := tempMemTable.Front()
	for node != nil {
		if len(node.Key()) <= 0 {
			node = node.Next()
			continue
		}

		if tableBuilder.Size() >= nextLevelTableSize {
			targetLevel := level
			need, _ := storage.needCompaction(level)
			if need {
				targetLevel = nextLevel
			}

			filePathOnLevelPrefix := filePathPrefix + strconv.Itoa(targetLevel) + "/"
			storage.writeToFile(targetLevel, tableBuilder, nextLevelTableSize, filePathOnLevelPrefix)
		}

		tableBuilder.Add([]byte(node.Key()), node.Value())
		node = node.Next()
	}

	// wrtie remained data to file
	filePathOnLevelPrefix := filePathPrefix + strconv.Itoa(nextLevel) + "/"
	storage.writeToFile(nextLevel, tableBuilder, nextLevelTableSize, filePathOnLevelPrefix)
}
