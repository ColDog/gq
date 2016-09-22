package store

import (
	"fmt"
	"github.com/coldog/go-graph/objects"
	"github.com/coldog/go-graph/store/bolt"
	"testing"
)

func storeTestSimpleNode(store Store) error {
	for i := 0; i < 10; i++ {
		n := objects.NewNode("test")
		store.Put(&objects.Object{n.Key(), n.Body})
	}

	ch, err := store.Prefix("test_", 10)
	if err != nil {
		return err
	}

	c := 0
	for n := range ch {
		if n.Type() != objects.NodeType {
			return fmt.Errorf("type is not node %v", n.Type())
		}
		c++
	}

	if c != 10 {
		return fmt.Errorf("not right count %v", c)
	}

	return nil
}

func storeTestRelations(store Store) error {
	n := objects.NewNode("test1")
	n.ID = "1234"

	store.Put(&objects.Object{n.Key(), n.Body})

	for i := 0; i < 20; i++ {
		n2 := objects.NewNode("test2")
		e := objects.NewEdge("likes", n.Key(), n2.Key())
		store.Put(
			&objects.Object{e.ForwardKey(), e.Body},
			&objects.Object{e.ReverseKey(), e.Body},
		)
	}

	ch, err := store.Prefix("1test1_1234/likes/", 20)
	if err != nil {
		return err
	}

	c := 0
	for n := range ch {
		if n.Type() != objects.EdgeType {
			return fmt.Errorf("type is not edge %v", n.Type())
		}
		c++
	}

	if c != 20 {
		return fmt.Errorf("not right count %v", c)
	}

	return nil
}

func TestBoltStore(t *testing.T) {
	store := bolt.NewBoltStore("test")
	store.Open()
	defer store.Close()
	defer store.Drop()

	err := storeTestSimpleNode(store)
	if err != nil {
		t.Fatal(err)
	}

	err = storeTestRelations(store)

	if err != nil {
		t.Fatal(err)
	}

	store.Debug()
}
