package hashring

import (
	"testing"
)

var (
	manager   = NewManager[string](10)
	initNodes = []string{
		"test1",
		"test2",
		"test3",
		"test4",
		"test5",
		"test6",
	}
)

func init() {
	for _, v := range initNodes {
		node := NewNode(v, v)
		manager.Add(node)
	}
}

func TestWeight(t *testing.T) {
	t.Run("Should get replicas count", func(t *testing.T) {
		count := manager.hashRing.Count()
		if count != 6*10 {
			t.Fail()
		}
	})

	t.Run("Should change replicas count", func(t *testing.T) {
		node, ok := manager.GetNode("test3")
		if !ok {
			t.Fail()
			return
		}

		node.SetWeight(10)
		count := manager.hashRing.Count()
		if count != 150 {
			t.Fail()
		}
	})
}

func TestFind(t *testing.T) {
	t.Run("Should find current node", func(t *testing.T) {
		node, ok := manager.FindOne("test1-0")
		if !ok {
			t.Fail()
			return
		}

		if node.GetValue() != "test1" {
			t.Fail()
		}
	})

	t.Run("Should find next node", func(t *testing.T) {
		node, ok := manager.FindOne("test")
		if !ok {
			t.Fail()
			return
		}

		if node.GetValue() != "test6" {
			t.Fail()
		}
	})

	t.Run("Should not find the node", func(t *testing.T) {
		mgr := NewManager[string](10)
		_, ok := mgr.FindOne("test")
		if ok {
			t.Fail()
		}
	})

	t.Run("Should find next nodes", func(t *testing.T) {
		nodes := manager.FindNext("test1", 5)
		t.Log(len(nodes))
		if len(nodes) != 5 {
			t.Fail()
		}
	})

	t.Run("Should find prev nodes", func(t *testing.T) {
		nodes := manager.FindPrev("test", 5)
		if len(nodes) != 5 {
			t.Fail()
		}
	})

	t.Run("Should not find replica nodes", func(t *testing.T) {
		node := manager.FindNext("test", 70)
		if len(node) != 60 {
			t.Fail()
		}
	})
}

func TestRemove(t *testing.T) {
	t.Run("Should remove node", func(t *testing.T) {
		_, ok := manager.GetNode("test2")
		if !ok {
			t.Fail()
			return
		}
	})
}
