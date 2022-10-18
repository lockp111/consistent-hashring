package hashring

import (
	"strconv"

	"github.com/lockp111/consistent-hashring/hashring"
)

type Node[T any] struct {
	Data    T
	manager *NodeManager[T]
	key     string
	weight  int
}

func NewNode[T any](key string, data T) *Node[T] {
	return &Node[T]{
		Data:   data,
		key:    key,
		weight: 1,
	}
}

// Virtuals returns virtual nodes
func (n *Node[T]) Virtuals(replicas int) []hashring.Slot[T] {
	var (
		total = n.weight * replicas
		slots = make([]hashring.Slot[T], 0, total)
	)
	for i := 0; i < total; i++ {
		key := n.key + "-" + strconv.FormatInt(int64(i), 10)
		slots = append(slots, hashring.NewSlot(key, n.Data))
	}
	return slots
}

// SetWeight
func (n *Node[T]) SetWeight(w int) {
	if n.manager == nil {
		n.weight = w
		return
	}

	// remove old node
	n.manager.Remove(n.key)
	n.weight = w
	n.manager.Add(n)
}
