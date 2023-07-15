package table

type Iterator struct {
	table    *Table
	blockNum int
	index    int
}

func newTableIterator(table *Table) *Iterator {
	iter := &Iterator{
		table:    table,
		index:    -1,
		blockNum: len(table.blockMetas),
	}
	return iter.Next()
}

func (iter *Iterator) Next() *Iterator {
	iter.index++
	if iter.index >= iter.blockNum {
		return nil
	}

	return iter
}
