package graph

import (
	"github.com/coldog/go-graph/objects"
	"github.com/coldog/go-graph/store"

	"sync"
)

const workers = 20

func traverse(p *Pipeline, s store.Store, t *Traversal) []*objects.Object {
	p.AddStep(func(in <-chan *objects.Object) <-chan *objects.Object {
		debug("adding step", t.NodeType, t.ID, t.Next)

		if t.Next == nil {
			if in == nil {
				out, err := s.Prefix(
					concat(t.NodeType, objects.NodeSep, t.ID),
					t.LimitBy,
				)
				if err != nil {
					panic(err)
				}

				return out
			} else {
				out := make(chan *objects.Object)
				go func() {
					defer close(out)
					for o := range in {
						if !o.IsNode() {
							continue
						}

						n := o.Node()
						if t.NodeType != "" && t.NodeType != n.Type {
							continue
						}

						out <- o
					}
				}()

				return out
			}
		}

		out := make(chan *objects.Object)

		if in == nil {
			chans := []<-chan *objects.Object{}
			if t.ID == "" {
				chans = append(chans, query(t, s, t.NodeType, objects.NodeSep)...)
			} else {
				if len(t.Next.Types) == 0 {
					chans = append(chans, query(t, s, t.NodeType, objects.NodeSep, t.ID, objects.PathSep)...)
				}

				for _, nextType := range t.Next.Types {
					chans = append(chans, query(t, s, t.NodeType, objects.NodeSep, t.ID, objects.PathSep, nextType, objects.PathSep)...)
				}
			}

			merge(out, chans...)

		} else {
			mwg := &sync.WaitGroup{}
			mwg.Add(workers)
			for i := 0; i < workers; i++ {
				go func() {
					wg := sync.WaitGroup{}
					chans := []<-chan *objects.Object{}

					for o := range in {
						n := o.Node()

						if len(t.Next.Types) == 0 {
							chans = append(chans, query(t, s, n.Type, objects.NodeSep, n.ID, objects.PathSep)...)
						}

						for _, nextType := range t.Next.Types {
							chans = append(chans, query(t, s, n.Type, objects.NodeSep, n.ID, objects.PathSep, nextType, objects.PathSep)...)
						}

						for _, ch := range chans {
							wg.Add(1)
							go func() {
								for o := range ch {
									out <- o
								}
								wg.Done()
							}()
						}

						chans = []<-chan *objects.Object{}
					}

					wg.Wait()
					mwg.Done()
				}()

			}

			go func() {
				mwg.Wait()
				close(out)
			}()
		}

		return out
	})

	p.AddStep(func(in <-chan *objects.Object) <-chan *objects.Object {
		out := make(chan *objects.Object, 100)
		go func() {
			defer close(out)
			for o := range in {
				if o.IsNode() {
					out <- o
				}

				e := o.Edge()
				if t.Next != nil && len(t.Next.Types) > 0 && !inArray(e.Type, t.Next.Types) {
					continue
				}

				if t.Next != nil && t.Next.filter != nil && !t.Next.filter(e) {
					continue
				}

				if string(o.Key[0]) == objects.ForwardEdgeKey {
					out <- e.TargetNode().Object()
				} else if string(o.Key[0]) == objects.ReverseEdgeKey {
					out <- e.SourceNode().Object()
				}
			}
		}()
		return out
	})

	t.GroupBy("id")

	for _, a := range t.filters {
		p.AddStep(a)
	}

	if t.Next != nil {
		return traverse(p, s, t.Next.Target)
	}

	return p.Collect()
}

func debug(args ...interface{}) {
	//args = append([]interface{}{"D --> "}, args...)
	//fmt.Println(args...)
}

func concat(arg string, args ...string) string {
	for _, a := range args {
		arg += a
	}
	return arg
}

func query(t *Traversal, s store.Store, end ...string) []<-chan *objects.Object {
	switch t.Next.Dir {
	case objects.Out:
		res, err := s.Prefix(concat(objects.ForwardEdgeKey, end...), t.Next.LimitBy)
		if err != nil {
			panic(err)
		}
		return []<-chan *objects.Object{res}

	case objects.In:
		res, err := s.Prefix(concat(objects.ReverseEdgeKey, end...), t.Next.LimitBy)
		if err != nil {
			panic(err)
		}
		return []<-chan *objects.Object{res}

	case objects.Both:
		res1, err := s.Prefix(concat(objects.ReverseEdgeKey, end...), t.Next.LimitBy)
		if err != nil {
			panic(err)
		}
		res2, err := s.Prefix(concat(objects.ForwardEdgeKey, end...), t.Next.LimitBy)
		if err != nil {
			panic(err)
		}
		return []<-chan *objects.Object{res1, res2}

	}

	return nil
}

func merge(out chan *objects.Object, chans ...<-chan *objects.Object) {
	wg := &sync.WaitGroup{}
	wg.Add(len(chans))
	for _, ch := range chans {
		go func() {
			for o := range ch {
				out <- o
			}
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		close(out)
	}()
}

func inArray(s string, arr []string) bool {
	for _, item := range arr {
		if s == item {
			return true
		}
	}
	return false
}
