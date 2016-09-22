package graph

import (
	"fmt"
	"github.com/coldog/go-graph/objects"
	"github.com/coldog/go-graph/store"
	"log"
	"strings"
	"time"
)

func New(s store.Store) *Graph {
	g := &Graph{
		pipe:  &Pipeline{},
		store: s,
	}
	return g
}

type Graph struct {
	pipe  *Pipeline
	store store.Store
	T     *Traversal
}

func (g *Graph) PutByResourceID(resourceID string, body map[string]interface{}) (*objects.Object, error) {
	spl := strings.Split(resourceID, ":")
	if len(spl) <= 1 {
		return nil, fmt.Errorf("failed to parse %s", resourceID)
	}

	if spl[0] == "node" {
		// Node resourceID: node:<type>_<id>
		s := strings.Split(spl[1], objects.NodeSep)

		var id string
		if len(s) > 1 {
			id = s[1]
		} else if body["id"] != nil {
			if asserted, ok := body["id"].(string); ok {
				id = asserted
			}
		}

		if id == "" {
			id = objects.GenId()
		}

		n := &objects.Node{
			Type: s[0],
			ID:   id,
			Body: body,
		}
		return n.Object(), g.PutNode(n)

	} else if spl[0] == "edge" {
		// Edge resourceID: edge:<type>.<source>.<target>
		s := strings.Split(spl[1], ".")
		if len(s) < 3 {
			return nil, fmt.Errorf("failed to parse %s", resourceID)
		}

		e := &objects.Edge{
			Source: s[1],
			Target: s[2],
			Type:   s[0],
			Body:   body,
		}

		return e.Object(), g.PutEdge(e)
	}

	return nil, fmt.Errorf("failed to parse %s", resourceID)
}

func (g *Graph) GetByResourceID(resourceID string) (*objects.Object, error) {
	spl := strings.Split(resourceID, ":")
	if spl[0] == "node" {
		// Node resourceID: node:<type>_<id>
		return g.store.Get(spl[1])
	} else if spl[0] == "edge" {
		// Edge resourceID: edge:<type>.<source>.<target>
		s := strings.Split(spl[1], ".")
		return g.store.Get(concat(objects.ForwardEdgeKey, s[1], objects.PathSep, s[0], objects.PathSep, s[2]))
	}

	return nil, fmt.Errorf("failed to parse %s", resourceID)
}

func (g *Graph) DelByResourceID(resourceID string) (*objects.Object, error) {
	o, err := g.GetByResourceID(resourceID)
	if err != nil {
		return nil, err
	}

	return o, g.store.Del(o)
}

func (g *Graph) GetBody(key string) (map[string]interface{}, error) {
	o, err := g.store.Get(key)
	if err != nil {
		return nil, err
	}

	return o.Val, nil
}

func (g *Graph) CreateNode(t string, body map[string]interface{}) (*objects.Node, error) {
	n := objects.NewNode(t)
	n.Body = body
	err := g.PutNode(n)
	return n, err
}

func (g *Graph) CreateEdge(t, source, target string, body map[string]interface{}) (*objects.Edge, error) {
	e := objects.NewEdge(t, source, target)
	e.Body = body
	err := g.PutEdge(e)
	return e, err
}

func (g *Graph) PutNode(n *objects.Node) error {
	if n.Type == "" {
		return fmt.Errorf("node must have a type")
	} else if n.ID == "" {
		return fmt.Errorf("node must have an id")
	}

	return g.store.Put(&objects.Object{
		Key: n.Key(),
		Val: n.Body,
	})
}

func (g *Graph) PutEdge(e *objects.Edge) error {
	if e.Type == "" {
		return fmt.Errorf("edge must have a type")
	} else if e.Source == "" || e.Target == "" {
		return fmt.Errorf("edge is invalid, source or target is null %s", e.ResourceID())
	}

	return g.store.Put(
		&objects.Object{e.ForwardKey(), e.Body},
		&objects.Object{e.ReverseKey(), e.Body},
	)
}

func (g *Graph) DelNode(n *objects.Node) error {
	return g.store.Del(&objects.Object{n.Key(), nil})
}

func (g *Graph) DelEdge(e *objects.Edge) error {
	return g.store.Del(
		&objects.Object{e.ForwardKey(), nil},
		&objects.Object{e.ReverseKey(), nil},
	)
}

func (g *Graph) Traversal() *Traversal {
	return &Traversal{
		LimitBy: 100,
		G:       g,
	}
}

func (g *Graph) Flush() {
	g.store.Flush()
}

func (g *Graph) Run(t *Traversal) []*objects.Object {
	t1 := time.Now()
	p := &Pipeline{}
	res := traverse(p, g.store, t)
	t2 := time.Now()
	log.Printf("[INFO] graph: traversal with %d steps took %v returning %d nodes", len(p.steps), t2.Sub(t1), len(res))
	return res
}
