package hashring

import (
	"strconv"
	"sync"

	"github.com/lockp111/consistent-hashring/hashring"
)

type NodeManager[T any] struct {
	mux      sync.RWMutex
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

// Add node to manager
func (m *NodeManager[T]) Add(node *Node[T]) {
	m.mux.Lock()
	defer m.mux.Unlock()

	node.manager = m
	m.nodes[node.key] = *node
	m.hashRing.Add(node.Virtuals(m.replicas)...)
}

// GetNode returns manager node
func (m *NodeManager[T]) GetNode(key string) (*Node[T], bool) {
	m.mux.RLock()
	defer m.mux.RUnlock()

	node, ok := m.nodes[key]
	if !ok {
		return nil, false
	}
	return &node, true
}

// GetNodes returns total manager nodes
func (m *NodeManager[T]) GetNodes() []Node[T] {
	m.mux.RLock()
	defer m.mux.RUnlock()

	var list = make([]Node[T], 0, len(m.nodes))
	for _, node := range m.nodes {
		list = append(list, node)
	}
	return list
}

// Remove node by key
func (m *NodeManager[T]) Remove(nodeKey string) {
	m.mux.Lock()
	defer m.mux.Unlock()

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

// FindOne returns hashring slot by key
func (m *NodeManager[T]) FindOne(key string) (hashring.Slot[T], bool) {
	m.mux.RLock()
	defer m.mux.RUnlock()

	return m.hashRing.Get(key)
}

func (m *NodeManager[T]) find(key string, num int,
	fn func(hashring.Slot[T]) hashring.Slot[T]) []hashring.Slot[T] {
	m.mux.RLock()
	defer m.mux.RUnlock()

	var slots = make([]hashring.Slot[T], 0, num)
	slot, ok := m.hashRing.Get(key)
	if !ok {
		return slots
	}
	slots = append(slots, slot)

	// cache slot
	cache := map[uint32]bool{
		slot.Hash(): true,
	}

	var next = fn(slot)
	for i := 1; i < num; i++ {
		if cache[next.Hash()] {
			return slots
		}

		slots = append(slots, next)
		cache[next.Hash()] = true

		next = fn(next)
	}
	return slots
}

// FindNext returns hashring slots by key and next num
func (m *NodeManager[T]) FindNext(key string, num int) []hashring.Slot[T] {
	return m.find(key, num, m.hashRing.GetNext)
}

// FindPrev returns hashring slots by key and prev num
func (m *NodeManager[T]) FindPrev(key string, num int) []hashring.Slot[T] {
	return m.find(key, num, m.hashRing.GetPrev)
}

// Count returns manager nodes count
func (m *NodeManager[T]) Count() int {
	m.mux.RLock()
	defer m.mux.RUnlock()

	return len(m.nodes)
}

// Slots returns hashring slots count
func (m *NodeManager[T]) Slots() int {
	m.mux.RLock()
	defer m.mux.RUnlock()

	return m.hashRing.Count()
}
