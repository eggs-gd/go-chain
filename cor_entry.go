package chain

import (
	"context"
)

type EntryPoint[Ti any, To any] interface {
	worker
	Start(chan<- Ti, context.Context)
	Decorate(Ti) (To, error)
}

type entryRunner[Ti any, To any] struct {
	cherr     chan<- error
	chin      chan Ti
	chout     chan<- To
	processor EntryPoint[Ti, To]
}

func (d *entryRunner[Ti, To]) setErrorChannel(cherr chan<- error) {
	d.cherr = cherr
}

func (d *entryRunner[Ti, To]) Process(parentCtx context.Context) {
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	go d.processor.Start(d.chin, ctx)

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

func NewEntryPoint[Ti any, To any](chout chan<- To, processor EntryPoint[Ti, To]) Processor {

	return &entryRunner[Ti, To]{
		chin:      make(chan Ti),
		chout:     chout,
		processor: processor,
	}
}
