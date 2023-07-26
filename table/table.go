package table

import (
	"encoding/binary"
	"os"

	"github.com/ISSuh/lsm-tree/block"
	"github.com/ISSuh/lsm-tree/logging"
)

const (
	BlockMetaOffetTypeSize = 4
)

type Table struct {
	id               int
	path             string
	file             *os.File
	fileSize         int64
	blockMetas       []block.BlockMeta
	blockMetasOffset int
}

func OpenTable(id int, path string) *Table {
	file, err := os.Open(path)
	if err != nil {
		logging.Error("OpenTable - file open fail. ", path)
		return nil
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		logging.Error("OpenTable - invalid file. ", path)
		return nil
	}

	table := &Table{
		id:               id,
		path:             path,
		file:             file,
		fileSize:         info.Size(),
		blockMetas:       make([]block.BlockMeta, 0),
		blockMetasOffset: 0,
	}

	table.decodeBlockMetaOffset(table.fileSize)
	table.decodeBlockMetas(table.fileSize)
	return table
}

func (table *Table) NewTableLeader() *TableLeader {
	return &TableLeader{
		table: table,
	}
}

func (table *Table) Id() int {
	return table.id
}

func (table *Table) FileName() string {
	return table.file.Name()
}

func (table *Table) FirstKey() string {
	if len(table.blockMetas) <= 0 {
		return ""
	}
	return string(table.blockMetas[0].FirstKey())
}

func (table *Table) BlockNum() int {
	return len(table.blockMetas)
}

func (table *Table) Size() int64 {
	return table.fileSize
}

func (table *Table) LoadBlock(index int) *block.Block {
	if index >= len(table.blockMetas) {
		logging.Error("ReadBlock - invalid index. ", index)
		return nil
	}

	file, err := os.Open(table.path)
	if err != nil {
		logging.Error("OpenTable - file open fail. ", table.path)
		return nil
	}
	defer file.Close()

	table.file = file

	blockOffset := int(table.blockMetas[index].Offset())
	nextBlockOffset := 0

	nextIndex := index + 1
	if nextIndex < len(table.blockMetas) {
		nextBlockOffset = int(table.blockMetas[nextIndex].Offset())
	} else {
		nextBlockOffset = int(table.blockMetasOffset)
	}

	blockSize := nextBlockOffset - blockOffset
	blockBuffer := make([]byte, blockSize)
	n, err := table.file.ReadAt(blockBuffer, int64(blockOffset))
	if (err != nil) || (n != blockSize) {
		logging.Error("decodeBlockMetas - read error. erro : ", err, " / size n : ", n)
		return nil
	}

	block := &block.Block{}
	block.Decode(blockBuffer)
	return block
}

func (table *Table) decodeBlockMetaOffset(fileSize int64) {
	blockMetaOffsetByte := make([]byte, BlockMetaOffetTypeSize)
	n, err := table.file.ReadAt(blockMetaOffsetByte, fileSize-BlockMetaOffetTypeSize)
	if (err != nil) || (n != BlockMetaOffetTypeSize) {
		logging.Error("decodeBlockMetaOffset - read error. erro : ", err, " / size n : ", n)
		return
	}

	table.blockMetasOffset = int(binary.LittleEndian.Uint32(blockMetaOffsetByte))
}

func (table *Table) decodeBlockMetas(fileSize int64) {
	calculateblockMetasSize := fileSize - int64(table.blockMetasOffset+BlockMetaOffetTypeSize)
	blockMetasByte := make([]byte, calculateblockMetasSize)
	n, err := table.file.ReadAt(blockMetasByte, int64(table.blockMetasOffset))
	if (err != nil) || (n != int(calculateblockMetasSize)) {
		logging.Error("decodeBlockMetas - read error. erro : ", err, " / size n : ", n)
		return
	}

	table.blockMetas = block.DecodeBlockMetasFromByte(blockMetasByte)
}

type TableLeader struct {
	table *Table
}

func (leader *TableLeader) Get(key string) []byte {
	blockIndex := leader.findApproximateBlock(key)
	if blockIndex < 0 {
		return nil
	}

	block := leader.table.LoadBlock(blockIndex)
	if block == nil {
		return nil
	}

	return leader.searchOnBlock(key, block)
}

func (leader *TableLeader) findApproximateBlock(key string) int {
	blockIndex := -1
	for index, meta := range leader.table.blockMetas {
		firstKey := string(meta.FirstKey()[:])
		if key <= firstKey {
			blockIndex = index
			break
		}
	}
	return blockIndex
}

func (leader *TableLeader) searchOnBlock(key string, block *block.Block) []byte {
	iter := block.Iterator()
	for iter != nil {
		if key == iter.Key() {
			return iter.Value()
		}
		iter = iter.Next()
	}
	return nil
}
