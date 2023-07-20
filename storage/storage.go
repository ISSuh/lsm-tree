package storage

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/ISSuh/lsm-tree/logging"
	"github.com/ISSuh/lsm-tree/skiplist"
	"github.com/ISSuh/lsm-tree/table"
)

type Storage struct {
	option Option

	tableBuilder *table.TableBuilder

	memTable  *skiplist.SkipList
	immuTable *skiplist.SkipList

	mutex                  sync.RWMutex
	backgrounCompactSignal chan bool
	flushMemtableSignal    chan bool
	switchTable            chan bool
	terminateSync          sync.WaitGroup

	tableId map[int]int
	tables  []*table.Table
}

func NewStorage(option Option) *Storage {
	tableIdMap := make(map[int]int)
	for i := 0; i < option.Level; i++ {
		tableIdMap[i] = 0
	}

	storage := &Storage{
		option:                 option,
		tableBuilder:           table.NewTableBuilder(option.BlockSize, option.TableSize),
		memTable:               skiplist.New(option.LevelOnSkipList),
		immuTable:              nil,
		backgrounCompactSignal: make(chan bool, 100),
		flushMemtableSignal:    make(chan bool, 100),
		switchTable:            make(chan bool),
		tableId:                tableIdMap,
		tables:                 nil,
	}

	if !storage.createLevelDirectory() {
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
		logging.Error("Set - ", storage.memTable.Size(), " / ", storage.option.MemTableSize)
		<-storage.switchTable
	}
}

func (storage *Storage) Get(key string) []byte {
	return []byte("")
}

func (storage *Storage) Remove(key string) {
}

func (storage *Storage) Stop() {
	storage.flushMemtableSignal <- false
	storage.backgrounCompactSignal <- false
	storage.terminateSync.Wait()

	// storage.compact(storage.memTable)
}

func (storage *Storage) flushMemTable() {
	for run := range storage.flushMemtableSignal {
		logging.Info("flushMemTable - poasjdpa")

		immuTable := storage.memTable
		storage.memTable = skiplist.New(storage.option.LevelOnSkipList)
		if run {
			storage.switchTable <- true
		}

		storage.writeLevel0Table(immuTable)

		if !run {
			logging.Info("flushMemTable - signal is false.", run)
			break
		}
	}

	storage.terminateSync.Done()
	logging.Error("flushMemTable - done")
}

func (storage *Storage) backgroundCompact() {
	for run := range storage.backgrounCompactSignal {
		// storage.compact(immuTable)

		if !run {
			logging.Info("backgroundCompact - signal is false.", run)
			break
		}
	}

	storage.terminateSync.Done()
	logging.Error("backgroundCompact - done")
}

func (storage *Storage) writeLevel0Table(memTable *skiplist.SkipList) {
	logging.Error("writeLevel0Table")

	filePathPrefix := storage.option.Path + "/0/"

	node := memTable.Front()
	for node != nil {
		logging.Error("writeLevel0Table - key : ", node.Key())

		if storage.tableBuilder.Size() >= storage.option.TableSize {
			logging.Error("writeLevel0Table - ", storage.tableBuilder.Size(), " / ", storage.option.TableSize)
			storage.writeToFile(0, storage.tableBuilder, storage.option.TableSize, filePathPrefix)
		}

		storage.tableBuilder.Add([]byte(node.Key()), node.Value())
		node = node.Next()
	}

	// wrtie remained data to filez
	storage.writeToFile(0, storage.tableBuilder, storage.option.TableSize, filePathPrefix)
}

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

func (storage *Storage) createLevelDirectory() bool {
	for i := 0; i <= storage.option.Level; i++ {
		path := filepath.Join(storage.option.Path, strconv.Itoa(i))
		err := os.MkdirAll(path, os.ModePerm)
		if err != nil {
			logging.Error("checkNeedCompaction - can not read dir", path, ", err : ", err)
			return false
		}
	}
	return true
}

func (storage *Storage) removeMergedFile(level int, fileNames []string) {
	filePathPrefix := storage.option.Path + "/" + strconv.Itoa(level) + "/"
	for _, fileName := range fileNames {
		file := filePathPrefix + fileName
		_, err := os.Stat(file)
		if err == nil {
			os.Remove(file)
		}
	}
}

func (storage *Storage) writeToFile(level int, tableBuilder *table.TableBuilder, nextLevelTableSize int, filePathPrefix string) {
	file := filePathPrefix + strconv.Itoa(storage.tableId[level]) + ".db"
	newTable := tableBuilder.BuildTable(storage.tableId[level], file)
	storage.tables = append(storage.tables, newTable)

	// change to new TableBuilder
	tableBuilder = table.NewTableBuilder(storage.option.BlockSize, nextLevelTableSize)
	storage.tableId[level]++
}
