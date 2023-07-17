package storage

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func init() {

}

func TestMerge(t *testing.T) {
	path := "./.db"
	_, err := os.Stat(path)
	if err == nil {
		os.Remove(path)
	}

	assert.NotNil(t, path)

	os.Remove(path)
}
