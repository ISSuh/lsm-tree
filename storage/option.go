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

package storage

const (
	B  int = 1
	KB int = 1024
	MB int = 1024 * 1024
)

const (
	CurrentPath               = "./"
	DefaultLevel              = 7
	DefaultBlockSize          = 4
	DefaultTableSize          = 2
	DefaultLimitedFileNumOnL0 = 2
	DefaultTableSizeOffset    = 10
	DefaultLevelOnSkiplist    = 5
	DefaultMembtableSize      = 4
)

type Option struct {
	// default directory path where placed files
	Path string

	// max level of files
	// start level is 0
	// the last level has unlimit number of files
	Level int

	// max block size on table
	BlockSize int

	// max L0 table size
	TableSize int

	// limited number of files on level
	// key is level, value is limited number
	// the last level has unlimit number of files
	LimitedFilesNumOnL0 int

	// offset of calculated table size whne inscrease level
	// if tableSize value is 10Mb and tableSizeOffset value is 10,
	// the Max L0 file size is 10Mb and L1 file size is 100Mb(10Mb * 10)
	TableSizeOffset int

	// max level value on skiplist
	LevelOnSkipList int

	// limited number of memtable
	MemTableSize int
}

func NewOption() Option {
	return Option{
		Path:                CurrentPath,
		Level:               DefaultLevel,
		BlockSize:           DefaultBlockSize * B,
		TableSize:           DefaultTableSize * MB,
		LimitedFilesNumOnL0: DefaultLimitedFileNumOnL0,
		TableSizeOffset:     DefaultTableSizeOffset,
		LevelOnSkipList:     DefaultLevelOnSkiplist,
		MemTableSize:        DefaultMembtableSize * MB,
	}
}
