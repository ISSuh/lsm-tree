package storage

import (
	"io/ioutil"
	"strconv"

	"github.com/ISSuh/lsm-tree/logging"
	"github.com/ISSuh/lsm-tree/skiplist"
	"github.com/ISSuh/lsm-tree/table"
)

func (storage *Storage) tableFileListOnLevel0() []string {
	level := 0
	levelDirPath := storage.option.Path + "/" + strconv.Itoa(level)
	files, err := ioutil.ReadDir(levelDirPath)
	if err != nil {
		logging.Error("tableFileListOnLevel0 - can not read dir", levelDirPath, ", err : ", err)
		return nil
	}

	var fileNames []string
	for _, file := range files {
		fileNames = append(fileNames, file.Name())
	}
	return fileNames
}

// func (storage *Storage) needCompaction(level int) (bool, []string) {
// 	levelDirPath := storage.option.Path + "/" + strconv.Itoa(level)
// 	files, err := ioutil.ReadDir(levelDirPath)
// 	if err != nil {
// 		logging.Error("checkNeedCompaction - can not read dir", levelDirPath, ", err : ", err)
// 		return false, nil
// 	}

// if level == 0 {
// 	limitedFileSizeOnLevel
// } else {
// 	limitedFileSizeOnLevel := (storage.option.LimitedFilesNumOnL0 * storage.option.TableSize) * (level * storage.option.TableSizeOffset)
// }
// if len(files) < limitedFileSizeOnLevel {
// 	return false, nil
// }

// 	var fileNames []string
// 	for _, file := range files {
// 		fileNames = append(fileNames, file.Name())
// 	}
// 	return true, fileNames
// }

func (storage *Storage) compact(level int) {
	if level == 0 {
		fileNames := storage.tableFileListOnLevel0()
		storage.compactOnLevel0(level, fileNames)
	} else {
		// storage.compactOnLevel(level, fileNames)
	}

	// for level := 0; level < storage.option.Level; level++ {
	// 	need, fileNames := storage.needCompaction(level)
	// 	if need {
	// 		logging.Error("backgroundWork - will compact level ", level, " / ", fileNames)
	// 		if level == 0 {
	// 		} else {
	// 		}
	// util.RemoveMergedFile(storage.option.Path, level, fileNames)
	// }
	// }
}

func (storage *Storage) compactOnLevel0(level int, fileNames []string) {
	logging.Error("compactOnLevel0 - ", fileNames)

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

	firstKey := tempMemTable.Front().Key()
	lastKey := tempMemTable.Back().Key()

	targets := storage.findCompactionTargetOnNextLevel(level, firstKey, lastKey)
	logging.Error("compactOnLevel0 - ", targets)
}

// func (storage *Storage) compactOnLevel(level int, fileNames []string) {
// 	tempMemTable := skiplist.New(storage.option.LevelOnSkipList)

// 	filePathPrefix := storage.option.Path + "/" + strconv.Itoa(level) + "/"
// 	for _, fileName := range fileNames {
// 		filePath := filePathPrefix + fileName
// 		table := table.OpenTable(0, filePath)
// 		if table == nil {
// 			return
// 		}

// 		for i := 0; i < table.BlockNum(); i++ {
// 			block := table.LoadBlock(i)
// 			iter := block.Iterator()
// 			for iter != nil {
// 				tempMemTable.Set(iter.Key(), iter.Value())
// 				iter = iter.Next()
// 			}
// 		}
// 	}

// storage.buildTable(tempMemTable, level)
// }

func (storage *Storage) findCompactionTargetOnNextLevel(level int, begin, end string) []string {
	var targetTableFileName []string
	nextLevel := level + 1
	beginIndex, endIndex := 0, 0
	for i, table := range storage.tables[nextLevel] {
		firstKey := table.FirstKey()
		logging.Error("findCompactionTargetOnNextLevel - ", begin, " ~ ", end, " <= ", firstKey)

		if begin >= firstKey {
			beginIndex = i
			endIndex = i
		}

		if (begin <= end) && (end >= firstKey) {
			endIndex = i
		}
	}

	tables := storage.tables[nextLevel][beginIndex : endIndex+1]
	for _, table := range tables {
		targetTableFileName = append(targetTableFileName, strconv.Itoa(table.Id()))
	}
	return targetTableFileName
}

// func (storage *Storage) buildTable(tempMemTable *skiplist.SkipList, level int) {
// nextLevel := level + 1
// nextLevelTableSize := storage.option.TableSize * (nextLevel * storage.option.TableSizeOffset)
// tableBuilder := table.NewTableBuilder(storage.option.BlockSize)
// filePathPrefix := storage.option.Path + "/"

// node := tempMemTable.Front()
// for node != nil {
// 	if len(node.Key()) <= 0 {
// 		node = node.Next()
// 		continue
// 	}

// 	if tableBuilder.Size() >= nextLevelTableSize {
// 		targetLevel := level
// 		need, _ := storage.needCompaction(level)
// 		if need {
// 			targetLevel = nextLevel
// 		}

// 		filePathOnLevelPrefix := filePathPrefix + strconv.Itoa(targetLevel) + "/"
// 		storage.writeToFile(targetLevel, tableBuilder, nextLevelTableSize, filePathOnLevelPrefix)
// 	}

// 	tableBuilder.Add([]byte(node.Key()), node.Value())
// 	node = node.Next()
// }

// // wrtie remained data to file
// filePathOnLevelPrefix := filePathPrefix + strconv.Itoa(nextLevel) + "/"
// storage.writeToFile(nextLevel, tableBuilder, nextLevelTableSize, filePathOnLevelPrefix)
// }
