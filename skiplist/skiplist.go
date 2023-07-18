package skiplist

import (
	"math/rand"
	"sync"
	"time"
)

type SkipListItem struct {
	key   string
	value []byte
}

func (item *SkipListItem) Key() string {
	return item.key
}

func (item *SkipListItem) Value() []byte {
	return item.value
}

type SkipListNode struct {
	levels    int
	prevNode  []*SkipListNode
	nextNode  []*SkipListNode
	item      SkipListItem
	isEndNode bool
}

func (node *SkipListNode) Next() *SkipListNode {
	return node.nextNode[0]
}

func (node *SkipListNode) Prev() *SkipListNode {
	return node.prevNode[0]
}

func (node *SkipListNode) Key() string {
	return node.item.key
}

func (node *SkipListNode) Value() []byte {
	return node.item.value
}

func (node *SkipListNode) next(targetLevel int) *SkipListNode {
	if node.levels < targetLevel {
		return nil
	}
	return node.nextNode[targetLevel]
}

func (node *SkipListNode) match(key string) bool {
	return key == node.item.key
}

func (node *SkipListNode) nodeLevel() int {
	return node.levels
}

func (node *SkipListNode) appendOnLevel(newNode *SkipListNode, targetLevel int) {
	if node.nextNode[targetLevel] != nil {
		node.nextNode[targetLevel].prevNode[targetLevel] = newNode
	}

	newNode.prevNode[targetLevel] = node
	newNode.nextNode[targetLevel] = node.nextNode[targetLevel]

	node.nextNode[targetLevel] = newNode
}

func (node *SkipListNode) removeOnLevel(targetLevel int) {
	if node.nextNode[targetLevel] != nil {
		node.nextNode[targetLevel].prevNode[targetLevel] = node.prevNode[targetLevel]
	}

	if node.prevNode[targetLevel] != nil {
		node.prevNode[targetLevel].nextNode[targetLevel] = node.nextNode[targetLevel]
	}
}

type SkipList struct {
	maxLevel int
	length   int
	size     uint64
	head     *SkipListNode
	tail     *SkipListNode
	rand     *rand.Rand
	mutex    sync.RWMutex
	history  []*SkipListNode
}

func New(maxLevel int) *SkipList {
	headNode := &SkipListNode{
		levels:    maxLevel,
		prevNode:  make([]*SkipListNode, maxLevel),
		nextNode:  make([]*SkipListNode, maxLevel),
		item:      SkipListItem{},
		isEndNode: true,
	}

	tailNode := &SkipListNode{
		levels:    maxLevel,
		prevNode:  make([]*SkipListNode, maxLevel),
		nextNode:  make([]*SkipListNode, maxLevel),
		item:      SkipListItem{},
		isEndNode: true,
	}

	list := SkipList{
		maxLevel: maxLevel,
		length:   0,
		rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
		head:     headNode,
		tail:     tailNode,
		history:  make([]*SkipListNode, maxLevel),
	}

	for i := 0; i < maxLevel; i++ {
		list.head.appendOnLevel(list.tail, i)
	}

	return &list
}

func (list *SkipList) MaxLevel() int {
	return list.maxLevel
}

func (list *SkipList) Length() int {
	return list.length
}

func (list *SkipList) Size() uint64 {
	return list.size
}

func (list *SkipList) Front() *SkipListNode {
	return list.head.nextNode[0]
}

func (list *SkipList) Back() *SkipListNode {
	return list.tail.prevNode[0]
}

func (list *SkipList) Set(key string, value []byte) {
	node := list.findInternal(key, list.history)
	if node != nil {
		node.item.value = value
		return
	}

	list.insertNode(key, value, list.history)
}

func (list *SkipList) Get(key string) *SkipListItem {
	node := list.findInternal(key, list.history)
	if node == nil {
		return nil
	}
	return &node.item
}

func (list *SkipList) Remove(key string) {
	node := list.findInternal(key, list.history)
	if node == nil {
		return
	}

	list.deleteNode(node)
}

func (list *SkipList) findInternal(key string, history []*SkipListNode) *SkipListNode {
	list.mutex.Lock()
	defer list.mutex.Unlock()

	current := list.head
	for i := list.maxLevel - 1; i >= 0; i-- {
		for list.tail != current.next(i) && current.next(i).item.key < key {
			current = current.next(i)
		}
		history[i] = current
	}

	current = current.next(0)
	if current.isEndNode || !current.match(key) {
		return nil
	}
	return current
}

func (list *SkipList) insertNode(key string, value []byte, history []*SkipListNode) {
	randomLevel := list.randomLevel()

	node := &SkipListNode{
		levels:    randomLevel,
		prevNode:  make([]*SkipListNode, randomLevel),
		nextNode:  make([]*SkipListNode, randomLevel),
		item:      SkipListItem{key: key, value: value},
		isEndNode: false,
	}

	list.mutex.Lock()
	defer list.mutex.Unlock()

	for i := 1; i <= randomLevel; i++ {
		randomLevelIndex := i - 1
		history[randomLevelIndex].appendOnLevel(node, randomLevelIndex)
	}

	list.length++
	list.size += uint64(len(key))
	list.size += uint64(len(value))
}

func (list *SkipList) deleteNode(node *SkipListNode) {
	list.mutex.Lock()
	defer list.mutex.Unlock()

	list.size -= uint64(len(node.Key()))
	list.size -= uint64(len(node.Value()))

	for i := 0; i < node.nodeLevel(); i++ {
		node.removeOnLevel(i)
	}

	list.length--
}

func (list *SkipList) randomLevel() int {
	const prob = 1 << 30
	maxLevel := list.maxLevel
	rand := list.rand

	level := 1
	for ; (level < maxLevel) && (rand.Int31() > prob); level++ {
	}

	return level
}
