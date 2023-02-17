package hashring

import (
	"strconv"

	"github.com/lockp111/consistent-hashring/hashring"
)

type Node[T any] struct {
	Data   T
	key    string
	weight int
}

func NewNode[T any](key string, data T) *Node[T] {
	return &Node[T]{
		Data:   data,
		key:    key,
		weight: 1,
	}
}

func (n *Node[T]) virtualKey(index int) string {
	return n.key + "#" + strconv.FormatInt(int64(index), 10)
}

// Virtuals returns virtual nodes
func (n *Node[T]) Virtuals(replicas int) []hashring.Slot[T] {
	var (
		total = n.weight * replicas
		slots = make([]hashring.Slot[T], 0, total)
	)
	for i := 0; i < total; i++ {
		key := n.virtualKey(i)
		slots = append(slots, hashring.NewSlot(key, n.Data))
	}
	return slots
}

// VirtualKeys returns virtual keys
func (n *Node[T]) VirtualKeys(replicas int) []string {
	var (
		total = n.weight * replicas
		keys  = make([]string, 0, total)
	)
	for i := 0; i < total; i++ {
		keys = append(keys, n.virtualKey(i))
	}
	return keys
}

// SetWeight
func (n *Node[T]) SetWeight(w int) {
	n.weight = w
}

// GetWeight
func (n *Node[T]) GetWeight() int {
	return n.weight
}

// GetKey
func (n *Node[T]) GetKey() string {
	return n.key
}
