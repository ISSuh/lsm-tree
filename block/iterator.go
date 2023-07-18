package block

import (
	"github.com/ISSuh/lsm-tree/entry"
)

type Iterator struct {
	blcok *Block
	entry *entry.Entry
	index int
}

func newBlockIterator(block *Block) *Iterator {
	iter := &Iterator{
		blcok: block,
		entry: &entry.Entry{},
		index: -1,
	}
	return iter.Next()
}

func (iter *Iterator) Next() *Iterator {
	iter.index++
	if iter.index >= iter.blcok.entryNum {
		return nil
	}

	begin := iter.blcok.offsets[iter.index]
	var end int16 = 0

	nextIndex := iter.index + 1
	if nextIndex >= iter.blcok.entryNum {
		end = int16(len(iter.blcok.data))
	} else {
		end = iter.blcok.offsets[nextIndex]
	}

	buffer := iter.blcok.data[begin:end]
	iter.entry.Decode(buffer)
	return iter
}

func (iter *Iterator) Key() string {
	return iter.entry.Key()
}

func (iter *Iterator) Value() []byte {
	return iter.entry.Value()
}
