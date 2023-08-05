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
