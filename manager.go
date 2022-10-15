package hashring

import (
	"strconv"
	"sync"

	"github.com/lockp111/datastructure/hashring"
)

type NodeManager[T any] struct {
	sync.RWMutex
	nodes    map[string]Node[T]
	hashRing *hashring.HashRing[T]
	replicas int
}

func NewManager[T any](replicas int) *NodeManager[T] {
	return &NodeManager[T]{
		nodes:    make(map[string]Node[T]),
		hashRing: hashring.New[T](),
		replicas: replicas,
	}
}

func (m *NodeManager[T]) Add(node *Node[T]) {
	m.Lock()
	defer m.Unlock()

	node.manager = m
	m.nodes[node.key] = *node
	m.hashRing.Add(node.Virtuals(m.replicas)...)
}

func (m *NodeManager[T]) Remove(nodeKey string) {
	m.Lock()
	defer m.Unlock()

	node, ok := m.nodes[nodeKey]
	if !ok {
		return
	}

	delete(m.nodes, nodeKey)
	total := node.weight * m.replicas
	for i := 0; i < total; i++ {
		m.hashRing.Remove(node.key + "-" + strconv.FormatInt(int64(i), 10))
	}
}

func (m *NodeManager[T]) Get(key string) (hashring.Slot[T], bool) {
	m.RLock()
	defer m.RUnlock()

	return m.hashRing.Get(key)
}

func (m *NodeManager[T]) List(key string, count int) []hashring.Slot[T] {
	m.RLock()
	defer m.RUnlock()

	var slots = make([]hashring.Slot[T], 0, count)
	slot, ok := m.hashRing.Get(key)
	if !ok {
		return slots
	}

	cache := map[uint32]bool{
		slot.Hash(): true,
	}

	for i := 1; i < count; i++ {
		next := m.hashRing.GetNext(slot)
		if cache[next.Hash()] {
			return slots
		}

		slots = append(slots, m.hashRing.GetNext(slot))
		cache[next.Hash()] = true
	}
	return slots
}
