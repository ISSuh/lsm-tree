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

	mutex               sync.RWMutex
	backgrounWorkSignal chan bool
	switchTable         chan bool

	tableId int
	tables  []*table.Table
}

func NewStorage(option Option) *Storage {
	storage := &Storage{
		option:              option,
		tableBuilder:        table.NewTableBuilder(option.BlockSize, option.TableSize),
		memTable:            skiplist.New(option.LevelOnSkipList),
		immuTable:           nil,
		backgrounWorkSignal: make(chan bool),
		switchTable:         make(chan bool),
		tableId:             0,
		tables:              nil,
	}

	if !storage.createLevelDirectory() {
		return nil
	}

	go storage.backgroundWork()
	return storage
}

func (storage *Storage) Set(key string, value []byte) {
	storage.memTable.Set(key, value)
	if storage.memTable.Size() >= uint64(storage.option.MemTableSize) {
		storage.backgrounWorkSignal <- true

		<-storage.switchTable
	}
}

func (storage *Storage) Get(key string) []byte {
	return []byte("")
}

func (storage *Storage) Remove(key string) {
}

func (storage *Storage) backgroundWork() {
	for {
		signal := <-storage.backgrounWorkSignal
		if !signal {
			logging.Info("backgroundWork - signal is false.", signal)
			break
		}

		storage.immuTable = storage.memTable
		storage.memTable = skiplist.New(storage.option.LevelOnSkipList)
		storage.switchTable <- true

		storage.writeLevel0Table()

		for level := 0; level < storage.option.Level; level++ {
			if storage.needCompaction(level) {
				logging.Error("backgroundWork - will compact level ", level)
			}
		}
	}
}

func (storage *Storage) writeLevel0Table() {
	logging.Error("writeLevel0Table")

	filePathPrefix := storage.option.Path + "/0/"

	node := storage.immuTable.Front()
	for node != nil {
		if storage.tableBuilder.Size() >= storage.option.TableSize {
			logging.Error("writeLevel0Table - build table size : ", storage.tableBuilder.Size())
			file := filePathPrefix + strconv.Itoa(storage.tableId) + ".db"

			newTable := storage.tableBuilder.BuildTable(storage.tableId, file)
			storage.tables = append(storage.tables, newTable)

			storage.tableBuilder = table.NewTableBuilder(storage.option.BlockSize, storage.option.TableSize)
			storage.tableId++
		}

		storage.tableBuilder.Add([]byte(node.Key()), node.Value())

		node = node.Next()
	}

	logging.Error("writeLevel0Table - tableBuilder size : ", storage.tableBuilder.Size())
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

func (storage *Storage) needCompaction(level int) bool {
	levelDirPath := storage.option.Path + "/" + strconv.Itoa(level)
	files, err := ioutil.ReadDir(levelDirPath)
	if err != nil {
		logging.Error("checkNeedCompaction - can not read dir", levelDirPath, ", err : ", err)
		return false
	}

	if len(files) < storage.option.LimitedFilesNum[level] {
		return false
	}
	return true
}
