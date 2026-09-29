package chain

import (
	"context"
	"errors"
	"sync"
)

var ErrSkippedItem = errors.New("skipped item")

type worker interface {
	Stop()
}

type Processor interface {
	setErrorChannel(chan<- error)
	Process(context.Context)
}

type ChainProcessor interface {
	Processor
	AddStep(actor Processor)
}

type chain struct {
	errch  chan<- error
	actors []Processor
}

func (ch *chain) setErrorChannel(errch chan<- error) {
	ch.errch = errch
}

func (ch *chain) AddStep(a Processor) {
	a.setErrorChannel(ch.errch)
	ch.actors = append(ch.actors, a)
}

func (ch *chain) Process(parentCtx context.Context) {
	wg := &sync.WaitGroup{}

	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	for _, actor := range ch.actors {
		wg.Add(1)
		go func(s Processor) {
			defer wg.Done()
			s.Process(ctx)
		}(actor)
	}

	<-ctx.Done()
	wg.Wait()

}

func NewChainProcessor(errch chan error) *chain {
	ch := &chain{}
	ch.setErrorChannel(errch)
	return ch
}
