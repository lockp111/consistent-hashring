package hashring

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"sort"
	"testing"
)

type node struct {
	key   string
	value int
}

var nodeList = []Slot[node]{
	NewSlot("1", node{key: "1", value: 1}),
	NewSlot("2", node{key: "2", value: 2}),
	NewSlot("3", node{key: "3", value: 3}),
	NewSlot("4", node{key: "4", value: 4}),
	NewSlot("5", node{key: "5", value: 5}),
	NewSlot("6", node{key: "6", value: 6}),
}

var sortNodes = []node{
	{key: "2", value: 2},
	{key: "6", value: 6},
	{key: "3", value: 3},
	{key: "1", value: 1},
	{key: "5", value: 5},
	{key: "4", value: 4},
}

func TestHashRingGet(t *testing.T) {
	ring := New[node]()
	ring.Add(nodeList...)

	slot, ok := ring.Get("2")
	if !ok {
		t.Fatal()
	}

	if slot.Hash() != nodeList[1].Hash() {
		t.Fatal()
	}

	slot, _ = ring.Get("7")
	if slot.Hash() != nodeList[2].Hash() {
		t.Fatal()
	}
}

func TestHashRingRemove(t *testing.T) {
	ring := New[node]()
	ring.Add(nodeList...)

	slot, ok := ring.Get("4")
	if !ok {
		t.Fatal()
	}
	if slot.Hash() != nodeList[3].Hash() {
		t.Fatal()
	}

	ring.Remove("4")

	slot, _ = ring.Get("4")
	if slot.Hash() != nodeList[1].Hash() {
		t.Fatal()
	}
}

func TestUnsortAdd(t *testing.T) {
	ring := New[node]()
	ring.UnsortAdd(nodeList...)
	ring.Sort()

	ring.ForEach(func(index int, hash uint32, value node) {
		node := sortNodes[index]
		if node != value {
			t.Fatal()
		}
	})
}

func TestGetNext(t *testing.T) {
	ring := New[node]()
	for _, n := range nodeList {
		ring.Add(n)
	}

	slot, ok := ring.Get("3")
	if !ok {
		t.Fatal()
	}
	if slot.GetValue() != sortNodes[2] {
		t.Fatal()
	}

	slot = ring.GetNext(slot)
	if slot.GetValue() != sortNodes[3] {
		t.Fatal()
	}

	index := 3
	for i := 0; i < ring.Count(); i++ {
		index++
		if index >= ring.Count() {
			index = 0
		}
		slot = ring.GetNext(slot)
		if slot.GetValue() != sortNodes[index] {
			t.Fatal()
		}
	}
}

func TestGetPrev(t *testing.T) {
	ring := New[node]()
	for _, n := range nodeList {
		ring.Add(n)
	}

	slot, ok := ring.Get("3")
	if !ok {
		t.Fatal()
	}
	if slot.GetValue() != sortNodes[2] {
		t.Fatal()
	}

	slot = ring.GetPrev(slot)
	if slot.GetValue() != sortNodes[1] {
		t.Fatal()
	}

	index := 1
	for i := 0; i < ring.Count(); i++ {
		index--
		if index < 0 {
			index = ring.Count() - 1
		}
		slot = ring.GetPrev(slot)
		if slot.GetValue() != sortNodes[index] {
			t.Fatal()
		}
	}
}

func BenchmarkAdd(b *testing.B) {
	var slots []Slot[node]
	for j := 0; j < 1000000; j++ {
		n := node{fmt.Sprint(j), j}
		slots = append(slots, NewSlot(fmt.Sprint(j), n))
	}

	b.Run("add one", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ring := New[node]()
			for _, s := range slots {
				ring.UnsortAdd(s)
			}
			sort.Sort(ring)
		}
	})

	b.Run("batch add", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ring := New[node]()
			ring.Add(slots...)
		}
	})

}

func BenchmarkRemove(b *testing.B) {
	ring := New[node]()
	var slots []Slot[node]
	for i := 0; i < 1000000; i++ {
		n := node{fmt.Sprint(i), i}
		slots = append(slots, NewSlot(fmt.Sprint(i), n))
	}
	ring.Add(slots...)

	n := 500
	b.Run("remove", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			x := i * n
			for j := 0; j < n; j++ {
				ring.Remove(fmt.Sprint(j + x))
			}
		}
	})
}

func BenchmarkGet(b *testing.B) {
	ring := New[node]()
	var slots []Slot[node]
	for i := 0; i < 1000000; i++ {
		n := node{fmt.Sprint(i), i}
		slots = append(slots, NewSlot(fmt.Sprint(i), n))
	}
	ring.Add(slots...)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key, _ := rand.Int(rand.Reader, big.NewInt(1000000))
		ring.Get(key.String())
	}
}
