package storage

import (
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/ISSuh/lsm-tree/logging"
)

const (
	DbPath = "./test"
)

func GenRadomValue(min int, max int) int {
	return min + rand.Intn(max-min)
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func ClearDbDir() {
	_, err := os.Stat(DbPath)
	if err == nil {
		os.RemoveAll(DbPath)
	}
}

func ClearFiles(files []string) {
	for _, file := range files {
		_, err := os.Stat(file)
		if err == nil {
			os.Remove(file)
		}
	}
}

func TestMerge(t *testing.T) {
	ClearDbDir()

	// storage := NewStorage(NewOption())

	ClearDbDir()
}

func TestSet(t *testing.T) {
	ClearDbDir()

	option := NewOption()
	option.Path = DbPath

	logging.Error(option)
	storage := NewStorage(option)

	for i := 0; i <= 200; i++ {
		random := GenRadomValue(0, 1000000)
		keyAndValue := strconv.Itoa(random)

		storage.Set(keyAndValue, []byte(keyAndValue))
	}

	// ClearDbDir()
}
