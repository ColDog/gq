package graph

import "github.com/coldog/go-graph/objects"

type Step func(in <-chan *objects.Object) <-chan *objects.Object

type Pipeline struct {
	steps   []Step
	results []*objects.Object
}

func (p *Pipeline) emit(o *objects.Object) {
	p.results = append(p.results, o)
}

func (p *Pipeline) AddStep(s Step) {
	p.steps = append(p.steps, s)
}

func (p *Pipeline) Collect() []*objects.Object {
	var next <-chan *objects.Object = nil
	for _, stage := range p.steps {
		next = stage(next)
	}

	for obj := range next {
		p.results = append(p.results, obj)
	}
	return p.results
}
