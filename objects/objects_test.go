package objects

import (
	"fmt"
	"testing"
)

func TestNode(t *testing.T) {
	n := NewNode("user")
	fmt.Println(string(n.Key()))

	o := Object{
		Key: n.Key(),
		Val: n.Body,
	}

	if o.Type() != NodeType {
		t.Fatal("type is not node")
	}

	if string(n.Key()) != string(o.Node().Key()) {
		t.Fatal("after serialization don't match")
	}
}

func TestEdge(t *testing.T) {
	n := NewEdge("test", "1234", "5678")

	fmt.Println(string(n.ForwardKey()))
	fmt.Println(string(n.ReverseKey()))

	orev := Object{
		Key: n.ReverseKey(),
		Val: n.Body,
	}

	if orev.Type() != EdgeType {
		t.Fatal("type is not edge")
	}

	fmt.Printf("%+v\n", orev.Edge())

	if string(n.ForwardKey()) != string(orev.Edge().ForwardKey()) {
		t.Fatal("after serialization don't match")
	}

	if string(n.ReverseKey()) != string(orev.Edge().ReverseKey()) {
		t.Fatal("after serialization don't match")
	}

}
