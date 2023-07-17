package storage

type Option struct {
	// default directory path where placed files
	path string

	// max level of files
	level int

	// max block size on table
	blockSize int

	// max L0 table size
	tableSize int

	// limited number of files on level
	// key is level, value is limited number
	limitedFilesNum map[int]int

	// offset of calculated table size whne inscrease level
	// if tableSize value is 10Mb and tableSizeOffset value is 10,
	// the Max L0 file size is 10Mb and L1 file size is 100Mb(10Mb * 10)
	tableSizeOffset int

	// max level value on skiplist
	levelOnSkipList int
}
