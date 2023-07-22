package storage

import (
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/ISSuh/lsm-tree/logging"
	"github.com/stretchr/testify/assert"
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

	for i := 0; i <= 100; i++ {
		random := i
		keyAndValue := strconv.Itoa(random)

		storage.Set(keyAndValue, []byte(keyAndValue))
	}

	logging.Error("run Get")

	for i := 0; i <= 100; i++ {
		random := i
		keyAndValue := strconv.Itoa(random)

		data := storage.Get(keyAndValue)
		if assert.NotNil(t, data) {
			assert.Equal(t, string(data), keyAndValue)
		}
	}

	storage.Stop()
	// ClearDbDir()
}
