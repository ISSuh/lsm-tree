package storage

import (
	"github.com/ISSuh/skiplist"
)

type Storage struct {
	option   Option
	memTable *skiplist.SkipList
}

func NewStorage(option Option) *Storage {
	return &Storage{
		option: option,
	}
}

func (storage *Storage) Set() {
}

func (storage *Storage) Get() {
}

func (storage *Storage) Remove() {
}
