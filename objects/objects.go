package objects

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type Direction int

const (
	Out Direction = iota
	In
	Both
)

type ObjectType int

const (
	NoneType ObjectType = iota
	NodeType
	EdgeType
)

const (
	ForwardEdgeKey = "1"
	ReverseEdgeKey = "2"
	NodeSep        = "_"
	PathSep        = "/"
)

func concat(args ...string) (buff string) {
	for _, arg := range args {
		buff += arg
	}
	return
}

func NewNode(t string) *Node {
	return &Node{
		Type: t,
		ID:   GenId(),
	}
}

type Node struct {
	Type string                 `json:"type"`
	ID   string                 `json:"id"`
	Body map[string]interface{} `json:"body"`
	key  string                 `json:"key"`
}

func (n *Node) Key() string {
	if n.key == "" {
		n.key = concat(n.Type, NodeSep, n.ID)
	}

	return n.key
}

func (n *Node) Object() *Object {
	return &Object{n.Key(), n.Body}
}

func (n *Node) ResourceID() string {
	return fmt.Sprintf("node:%s", n.Key())
}

func (n *Node) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Key  string                 `json:"key"`
		ID   string                 `json:"id"`
		Type string                 `json:"type"`
		Body map[string]interface{} `json:"body"`
	}{
		Key:  n.Key(),
		ID:   n.ID,
		Type: n.Type,
		Body: n.Body,
	})
}

func NewEdge(t, source, target string) *Edge {
	return &Edge{
		Source: source,
		Target: target,
		Type:   t,
	}
}

type Edge struct {
	Source string                 `json:"source"`
	Target string                 `json:"target"`
	Type   string                 `json:"type"`
	Body   map[string]interface{} `json:"body"`
	forKey string
	revKey string
}

func (e *Edge) TargetNode() *Node {
	spl := strings.Split(e.Target, NodeSep)
	return &Node{
		Type: spl[0],
		ID:   spl[1],
	}
}

func (e *Edge) SourceNode() *Node {
	spl := strings.Split(e.Source, NodeSep)
	return &Node{
		Type: spl[0],
		ID:   spl[1],
	}
}

func (n *Edge) ForwardKey() string {
	if n.forKey == "" {
		n.forKey = concat(ForwardEdgeKey, n.Source, PathSep, n.Type, PathSep, n.Target)
	}

	return n.forKey
}

func (n *Edge) ReverseKey() string {
	if n.revKey == "" {
		n.revKey = concat(ReverseEdgeKey, n.Target, PathSep, n.Type, PathSep, n.Source)
	}

	return n.revKey
}

func (e *Edge) ResourceID() string {
	return fmt.Sprintf("edge:%s.%s.%s", e.Type, e.Source, e.Target)
}

func (e *Edge) Object() *Object {
	return &Object{e.ForwardKey(), e.Body}
}

type Object struct {
	Key string
	Val map[string]interface{}
}

func (o *Object) Type() ObjectType {
	if len(o.Key) == 0 {
		return NoneType
	}

	if string(o.Key[0]) == ReverseEdgeKey {
		return EdgeType
	} else if string(o.Key[0]) == ForwardEdgeKey {
		return EdgeType
	} else {
		return NodeType
	}
}

func (o *Object) IsNode() bool {
	return o.Type() == NodeType
}

func (o *Object) IsEdge() bool {
	return o.Type() == EdgeType
}

func (o *Object) Node() *Node {
	if o.Type() == NodeType {
		n := &Node{}

		spl := strings.Split(o.Key, NodeSep)
		n.Type = spl[0]
		n.ID = spl[1]
		n.Body = o.Val

		return n
	}

	return nil
}

func (o *Object) Edge() *Edge {
	if o.Type() == EdgeType {
		e := &Edge{}

		spl := strings.Split(string(o.Key[1:]), PathSep)

		if string(o.Key[0]) == ReverseEdgeKey {
			e.Source = spl[2]
			e.Target = spl[0]
			e.Type = spl[1]
		} else if string(o.Key[0]) == ForwardEdgeKey {
			e.Source = spl[0]
			e.Target = spl[2]
			e.Type = spl[1]
		}

		e.Body = o.Val

		return e
	}

	return nil
}

func (o *Object) MarshalJSON() (data []byte, err error) {
	if o.IsNode() {
		n := o.Node()
		data, err = json.Marshal(struct {
			Type       string `json:"type"`
			ResourceID string `json:"resource_id"`
			Node       *Node  `json:"node"`
		}{
			Type:       "node",
			Node:       n,
			ResourceID: n.ResourceID(),
		})
	} else if o.IsEdge() {
		e := o.Edge()
		data, err = json.Marshal(struct {
			Type       string `json:"type"`
			ResourceID string `json:"resource_id"`
			Edge       *Edge  `json:"edge"`
		}{
			Type:       "edge",
			ResourceID: e.ResourceID(),
			Edge:       e,
		})
	} else {
		data = []byte(`{"type": "none"}`)
	}
	return
}

func GenId() string {
	r := rand.Int63n(9000000000000000000)
	return fmt.Sprintf("%vn", r)
}
