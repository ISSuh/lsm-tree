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

func (table *Table) Iterator() *Iterator {
	return newTableIterator(table)
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
