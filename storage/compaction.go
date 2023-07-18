package storage

import (
	"encoding/json"
	"sync"

	"github.com/ISSuh/lsm-tree/logging"
	"github.com/ISSuh/lsm-tree/skiplist"
	"github.com/ISSuh/lsm-tree/table"
)

func (storage *Storage) Compact() {
}

func (storage *Storage) CompactOnLevel(level int) {
	// tempMemTable := skiplist.New(storage.option.levelOnSkipList)

	// workDir := storage.option.path + "/" + strconv.Itoa(level)
	// files, err := ioutil.ReadDir(workDir)
	// if err != nil {
	// 	logging.Error("CompactOnLevel - invalid dir path. ", workDir)
	// 	return
	// }

	// storage.Merge([2]string{files[0].Name(), files[1].Name()})
}

func (storage *Storage) Merge(lhs, rhs string, tempMemTable *skiplist.SkipList) {
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

	storage.writeToTable(tempMemTable)
}

func (storage *Storage) writeToTable(tempMemTable *skiplist.SkipList) {
	tableBuilder := table.NewTableBuilder(storage.option.BlockSize, storage.option.TableSize)

	node := tempMemTable.Front()
	for node != nil {
		value, err := json.Marshal(node.Value())
		if err != nil {
			continue
		}

		tableBuilder.Add([]byte(node.Key()), []byte(value))
		node = node.Next()
	}
}
