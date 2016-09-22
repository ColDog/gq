package graph

import (
	"fmt"
	"github.com/coldog/go-graph/objects"
	"github.com/coldog/go-graph/store/bolt"
	"testing"
)

func TestPipeline_Queries(t *testing.T) {
	p := &Pipeline{}
	store := bolt.NewBoltStore("test")
	store.Open()
	defer store.Close()
	defer store.Drop()

	n := objects.NewNode("test2")
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

	p.AddStep(func(in <-chan *objects.Object) <-chan *objects.Object {
		out := make(chan *objects.Object, 1)
		defer close(out)
		obj, _ := store.Get("test2_1234")
		out <- obj
		return out
	})

	p.AddStep(func(in <-chan *objects.Object) <-chan *objects.Object {
		out := make(chan *objects.Object)
		go func() {
			defer close(out)
			for o := range in {
				ch, _ := store.Prefix(
					concat(objects.ForwardEdgeKey, o.Key, objects.PathSep, "likes"),
					20,
				)

				for ob := range ch {
					out <- ob
				}
			}
		}()

		return out
	})

	list := p.Collect()
	for _, obj := range list {
		fmt.Printf("%s\n", obj.Key)
	}
}
