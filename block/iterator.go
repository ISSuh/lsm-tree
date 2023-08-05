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
	if iter.blcok == nil {
		return nil
	}

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
