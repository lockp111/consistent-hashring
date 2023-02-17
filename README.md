# consistent hashring
consistent hashring for golang(>=1.18)

## Usage
```shell
go get -u github.com/lockp111/consistent-hashring
```
### NewManager
```golang
func main(){
    type NodeInfo struct{
        Host string
        Name string
    }
    // new manager and set base virtual node count for every node
    manager = NewManager[NodeInfo](100)
}
```

### Add & Remove & SetWeight
```golang
func main(){
    nodeInfo := &NodeInfo{
        Host: "127.0.0.1"
        Name: "localhost"
    }

    node := NewNode(node.Name, nodeInfo)
    // change weight, default 1
    node.SetWeight(10)
    manager.Add(node)

    manager.Remove(nodeInfo.Name)
}
```

### GetNode & GetNodes
```golang
func main(){
    node, ok := manager.GetNode("localhost")
    nodes := manager.GetNodes()
}
```

### FindOne
```golang
func main(){
    slot, ok := manager.FindOne("xxxxx")
    nodeInfo := slot.GetValue()
}
```

### FindNext & FindPrev
```golang
func main(){
    slots := manager.FindNext("xxxxx", 10) //or slots := manager.FindPrev("xxxxx", 10)
    for _, slot := range slots {
        nodeInfo := slot.GetValue()
    }
}
```