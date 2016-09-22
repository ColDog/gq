package tests

import (
	"encoding/json"
	"fmt"
	"github.com/coldog/go-graph/graph"
	"github.com/coldog/go-graph/objects"
	"github.com/coldog/go-graph/store"
	"testing"
	"time"
)

type scenario func(t *testing.T, g *graph.Graph)

var scenarios = map[string]scenario{
	"simple":             simple,
	"social-graph:posts": socialGraphPosts,
	//"social-graph:multiple": socialGraphMultipleRelations,
	"social-graph:likes":           socialGraphLikes,
	"social-graph:likes-back":      socialGraphLikesBack,
	"social-graph:filter":          socialGraphFilter,
	"social-graph:post-serialized": socialGraphPostsSerialized,
	"social-graph:post-body":       socialGraphPostsWithBody,
}

var order = []string{
	"simple",
	"social-graph:posts",
	"social-graph:likes",
	"social-graph:likes-back",
	"social-graph:filter",
	"social-graph:post-serialized",
	"social-graph:post-body",
}

func RunTest(t *testing.T, s store.Store, name string) {
	g := graph.New(s)

	scen := scenarios[name]
	ok(t, s.Open())

	t1 := time.Now()
	fmt.Println("---> running:", name)
	scen(t, g)

	if t.Failed() {
		t2 := time.Now()
		fmt.Println("---> failed:", name, t2.Sub(t1))
		s.Close()
		s.Drop()
		return
	}

	t2 := time.Now()
	fmt.Println("---> passing:", name, t2.Sub(t1))

	s.Close()
	s.Drop()
}

func RunTests(t *testing.T, s store.Store) {
	for _, name := range order {
		RunTest(t, s, name)
	}
}

func simple(t *testing.T, g *graph.Graph) {
	n, err := g.CreateNode("test", nil)
	ok(t, err)

	for i := 0; i < 20; i++ {
		n2, err := g.CreateNode("test", nil)
		ok(t, err)
		g.CreateEdge("likes", n.Key(), n2.Key(), nil)
	}

	g.Flush()

	tr := g.Traversal()
	s := tr.Is("test").Out("likes").Count()

	//out, err := json.MarshalIndent(tr, " ", " ")
	//ok(t, err)

	//fmt.Printf("\n%s\n", out)

	equals(t, 20, s)
}

func socialGraphPosts(t *testing.T, g *graph.Graph) {
	seedSocial(t, g)

	tr := g.Traversal()
	tr.Is("user").Out("follows").Out("posts")
	list := tr.All()
	for _, o := range list {
		equals(t, "post", o.Node().Type)
	}

	_, err := json.MarshalIndent(tr, " ", " ")
	ok(t, err)

	//fmt.Printf("\n%s\n", out)

	equals(t, 20*5, len(list))
}

func socialGraphLikes(t *testing.T, g *graph.Graph) {
	seedSocial(t, g)

	list := g.Traversal().Is("user").Out("follows").Out("likes").All()
	for _, o := range list {
		equals(t, "post", o.Node().Type)
	}
	equals(t, 95, len(list))
}

//func socialGraphMultipleRelations(t *testing.T, g *graph.Graph) {
//	seedSocial(t, g)
//
//	list := g.Traversal().Is("user").Out("follows", "likes").All()
//	equals(t, 95, len(list))
//}

func socialGraphLikesBack(t *testing.T, g *graph.Graph) {
	seedSocial(t, g)

	list := g.Traversal().Is("user").Out("follows").Out("likes").In("posts").All()
	for _, o := range list {
		equals(t, "user", o.Node().Type)
	}
	equals(t, 19, len(list))
}

func socialGraphFilter(t *testing.T, g *graph.Graph) {
	seedSocial(t, g)

	list := g.Traversal().Is("user").OutFilter(func(e *objects.Edge) bool { return false }).All()
	equals(t, 0, len(list))
}

func socialGraphPostsSerialized(t *testing.T, g *graph.Graph) {
	seedSocial(t, g)

	q := `{
	  "next": {
	   "types": [
	    "follows"
	   ],
	   "direction": 0,
	   "target": {
	    "next": {
	     "types": [
	      "posts"
	     ],
	     "direction": 0,
	     "target": {
	      "next": null,
	      "id": "",
	      "limit": 2000
	     },
	     "limit": 2000
	    },
	    "id": "",
	    "limit": 2000
	   },
	   "limit": 2000
	  },
	  "type": "user",
	  "id": "",
	  "limit": 100
	 }`

	tr := g.Traversal()
	err := json.Unmarshal([]byte(q), tr)
	ok(t, err)

	list := tr.All()
	equals(t, 20*5, len(list))
}

func socialGraphPostsWithBody(t *testing.T, g *graph.Graph) {
	seedSocial(t, g)

	list := g.Traversal().Is("user").Out("follows").Out("posts").ForEach(func(n *objects.Node) {
		n.Body = map[string]interface{}{"Abc": "Def"}
	}).WithBody().All()
	equals(t, 20*5, len(list))
}

func seedSocial(t testing.TB, g *graph.Graph) {

	main, err := g.CreateNode("user", nil)
	main.ID = "main"
	ok(t, err)

	posts := []*objects.Node{}

	// create users
	for i := 0; i < 20; i++ {

		u, err := g.CreateNode("user", nil)
		ok(t, err)

		if i >= 15 {
			// users 15 - 20 like all the others posts
			for _, p := range posts {
				_, err = g.CreateEdge("likes", u.Key(), p.Key(), nil)
				ok(t, err)
			}
		}

		if i >= 10 {
			// users 10 - 20 dislike all the others posts
			for _, p := range posts {
				_, err = g.CreateEdge("dislike", u.Key(), p.Key(), nil)
				ok(t, err)
			}
		}

		// main user follows all others
		_, err = g.CreateEdge("follows", main.Key(), u.Key(), nil)
		ok(t, err)

		// every user has 5 posts
		for i := 0; i < 5; i++ {
			p, err := g.CreateNode("post", nil)
			ok(t, err)
			_, err = g.CreateEdge("posts", u.Key(), p.Key(), nil)
			ok(t, err)
			posts = append(posts, p)
		}
	}

	g.Flush()
}
