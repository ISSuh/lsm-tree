package util

import (
	"os"
	"path/filepath"
	"strconv"

	"github.com/ISSuh/lsm-tree/logging"
	"github.com/ISSuh/lsm-tree/table"
)

func CreateLevelDirectory(path string, MaxLevel int) bool {
	for i := 0; i <= MaxLevel; i++ {
		path := filepath.Join(path, strconv.Itoa(i))
		err := os.MkdirAll(path, os.ModePerm)
		if err != nil {
			logging.Error("checkNeedCompaction - can not read dir", path, ", err : ", err)
			return false
		}
	}
	return true
}

func RemoveFile(filePaths []string) {
	for _, filePath := range filePaths {
		_, err := os.Stat(filePath)
		if err == nil {
			os.Remove(filePath)
		}
	}
}

func RemoveTableFile(tables []*table.Table) {
	for _, table := range tables {
		_, err := os.Stat(table.FileName())
		if err == nil {
			os.Remove(table.FileName())
		}
	}
}

func TotalTableSizeOnLevel(path string, level int) int64 {
	levelDir := path + "/" + strconv.Itoa(level)
	var totalSize int64
	err := filepath.Walk(levelDir, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			totalSize += info.Size()
		}
		return nil
	})

	if err != nil {
		return 0
	}
	return totalSize
}
