package chain

import (
	"context"
)

type Switcher[Ti any, To any] interface {
	worker
	// Switch takes Ti and then makes decision where it should be placed
	// mapping evaluates by order channels in parent struct and results
	// arr := Switch(in Ti)
	// &Splitter.chout[0] <- arr[0]
	// &Splitter.chout[n] <- arr[n]
	Switch(Ti) (map[int]To, error)
	Stop()
}
type switchRunner[Ti any, To any] struct {
	cherr     chan<- error
	chin      <-chan Ti
	chout     []chan<- To
	processor Switcher[Ti, To]
}

func (s *switchRunner[Ti, To]) setErrorChannel(cherr chan<- error) {
	s.cherr = cherr
}

func (s *switchRunner[Ti, To]) Process(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			s.processor.Stop()
			return

		case input, ok := <-s.chin:
			if !ok {
				s.processor.Stop()
				return
			}
			res, err := s.processor.Switch(input)
			if err != nil {
				s.cherr <- err
			} else {
				for i, o := range res {
					if i < len(s.chout) {
						s.chout[i] <- o
					}
				}
			}
		}
	}
}

func NewSwitch[Ti any, To any](chin <-chan Ti, chout []chan<- To, processor Switcher[Ti, To]) Processor {
	return &switchRunner[Ti, To]{
		chin:      chin,
		chout:     chout,
		processor: processor,
	}
}
