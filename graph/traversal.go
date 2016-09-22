package graph

import (
	"github.com/coldog/go-graph/objects"
	"log"
)

type NodeFilter func(n *objects.Node) bool
type EdgeFilter func(e *objects.Edge) bool

type TraversalPath struct {
	Types   []string          `json:"types,omitempty"`
	Dir     objects.Direction `json:"direction"`
	Target  *Traversal        `json:"target"`
	LimitBy int               `json:"limit"`
	Filter  string            `json:"filter"`
	filter  EdgeFilter
}

type Traversal struct {
	Next     *TraversalPath `json:"next"`
	NodeType string         `json:"type,omitempty"`
	ID       string         `json:"id"`
	LimitBy  int            `json:"limit"`
	Filters  []string       `json:"filters"`
	G        *Graph         `json:"-"`
	root     *Traversal
	filters  []Step
}

func (t *Traversal) Out(relTypes ...string) *Traversal {
	return t.rel(objects.Out, nil, relTypes)
}

func (t *Traversal) In(relTypes ...string) *Traversal {
	return t.rel(objects.In, nil, relTypes)
}

func (t *Traversal) Both(relTypes ...string) *Traversal {
	return t.rel(objects.Both, nil, relTypes)
}

func (t *Traversal) OutFilter(f EdgeFilter) *Traversal {
	return t.rel(objects.Out, f, []string{})
}

func (t *Traversal) InFilter(f EdgeFilter) *Traversal {
	return t.rel(objects.In, f, []string{})
}

func (t *Traversal) BothFilter(f EdgeFilter) *Traversal {
	return t.rel(objects.Both, f, []string{})
}

func (t *Traversal) rel(dir objects.Direction, filter EdgeFilter, relType []string) *Traversal {
	var root *Traversal
	if t.root == nil {
		root = t
	} else {
		root = t.root
	}

	t.Next = &TraversalPath{
		Target:  &Traversal{LimitBy: 2000, G: t.G, root: root},
		Types:   relType,
		Dir:     dir,
		LimitBy: 2000,
		filter:  filter,
	}
	return t.Next.Target
}

func (t *Traversal) Is(nodeType string) *Traversal {
	t.NodeType = nodeType
	return t
}

func (t *Traversal) Has(attr string, value interface{}) *Traversal {
	if attr != "id" {
		panic("only id supported")
	}

	switch asserted := value.(type) {
	case string:
		t.ID = asserted
	case []byte:
		t.ID = string(asserted)
	default:
		panic("type not supported for ID")
	}
	return t
}

func (t *Traversal) Limit(s int) *Traversal {
	t.LimitBy = s
	return t
}

func (t *Traversal) Filter(f NodeFilter) *Traversal {
	t.Aggregate(func(in <-chan *objects.Object) <-chan *objects.Object {
		out := make(chan *objects.Object)
		go func() {
			defer close(out)
			for o := range in {
				n := o.Node()
				if f(n) {
					out <- o
				}
			}
		}()

		return out
	})
	return t
}

func (t *Traversal) Skip(s int) *Traversal {
	t.Aggregate(func(in <-chan *objects.Object) <-chan *objects.Object {
		c := 0
		out := make(chan *objects.Object)
		go func() {
			defer close(out)
			for o := range in {
				c++
				if c > s {
					out <- o
				}
			}
		}()

		return out
	})
	return t
}

func (t *Traversal) GroupBy(key string) *Traversal {
	t.Aggregate(func(in <-chan *objects.Object) <-chan *objects.Object {
		if in == nil {
			panic("cannot group on nil channel")
		}

		m := make(map[string]bool)
		out := make(chan *objects.Object)
		go func() {
			defer close(out)
			for o := range in {
				if !o.IsNode() {
					log.Println("[WARN] traversal: not piping node")
					continue
				}

				if key == "id" {
					id := o.Node().ID
					if m[id] {
						continue
					}

					m[id] = true
				} else {
					// todo:
				}

				out <- o
			}
		}()

		return out
	})
	return t
}

func (t *Traversal) WithBody() *Traversal {
	t.Aggregate(func(in <-chan *objects.Object) <-chan *objects.Object {
		out := make(chan *objects.Object)
		go func() {
			defer close(out)
			for o := range in {
				if o.Val == nil {
					bod, err := t.G.GetBody(o.Key)
					if err == nil {
						o.Val = bod
					}
				}

				out <- o
			}
		}()

		return out
	})
	return t
}

func (t *Traversal) ForEach(f func(n *objects.Node)) *Traversal {
	t.Aggregate(func(in <-chan *objects.Object) <-chan *objects.Object {
		out := make(chan *objects.Object)
		go func() {
			defer close(out)
			for o := range in {
				if o.IsNode() {
					f(o.Node())
				}
				out <- o
			}
		}()
		return out
	})
	return t
}

func (t *Traversal) Aggregate(a Step) *Traversal {
	t.filters = append(t.filters, a)
	return t
}

func (t *Traversal) All() []*objects.Object {
	var root *Traversal
	if t.root == nil {
		root = t
	} else {
		root = t.root
	}
	return t.G.Run(root)
}

func (t *Traversal) Count() int {
	var root *Traversal
	if t.root == nil {
		root = t
	} else {
		root = t.root
	}
	return len(t.G.Run(root))
}
