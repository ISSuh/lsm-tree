package storage

const (
	B  int = 1
	KB int = 1024
	MB int = 1024 * 1024
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
	LimitedFilesNum map[int]int

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
		Path:      "./",
		Level:     2,
		BlockSize: 10 * B,
		TableSize: 1 * KB, // 1KB
		LimitedFilesNum: map[int]int{
			0: 2,
			1: 4,
		},
		TableSizeOffset: 10,
		LevelOnSkipList: 5,
		MemTableSize:    100 * B,
	}
}
