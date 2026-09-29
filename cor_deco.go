package chain

import (
	"context"
)

type Decorator[Ti any, To any] interface {
	worker
	Decorate(Ti) (To, error)
}

type decoratorRunner[Ti any, To any] struct {
	cherr     chan<- error
	chin      <-chan Ti
	chout     chan<- To
	processor Decorator[Ti, To]
}

func (d *decoratorRunner[Ti, To]) setErrorChannel(cherr chan<- error) {
	d.cherr = cherr
}

func (d *decoratorRunner[Ti, To]) Process(parentCtx context.Context) {
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			d.processor.Stop()
			return

		case input, ok := <-d.chin:
			if !ok {
				d.processor.Stop()
				return
			}
			res, err := d.processor.Decorate(input)
			if err != nil {
				d.cherr <- err
			} else {
				d.chout <- res
			}
		}
	}
}

func NewDecorator[Ti any, To any](chin <-chan Ti, chout chan<- To, processor Decorator[Ti, To]) Processor {
	return &decoratorRunner[Ti, To]{
		chin:      chin,
		chout:     chout,
		processor: processor,
	}
}
