package util

import (
	"os"
	"path/filepath"
	"strconv"

	"github.com/ISSuh/lsm-tree/logging"
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

func RemoveMergedFile(pathPrefix string, level int, fileNames []string) {
	filePathPrefix := pathPrefix + "/" + strconv.Itoa(level) + "/"
	for _, fileName := range fileNames {
		file := filePathPrefix + fileName
		_, err := os.Stat(file)
		if err == nil {
			os.Remove(file)
		}
	}
}
