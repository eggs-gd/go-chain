// Package chain: steps that run concurrently, connected by channels. A chain has
// one input, its entry point; a pass (Process) runs every step until its input closes:
// the entry closes its output when it is done, a step that read its input to the
// end gives what it holds (Flusher) and returns, and an output closes once every
// step that writes to it has returned (a WaitGroup per output: where branches join,
// the joint closes after the last of them). Process returns when every step has.
package chain

import (
	"context"
	"errors"
	"sync"
)

// ErrSkippedItem: a step drops the value on purpose — not an error, never reported
var ErrSkippedItem = errors.New("skipped item")

// Processor: a step, or a chain of steps
type Processor interface {
	run(r runtime)
	// outputs: the channels the step writes to (a sub-chain closes its own)
	outputs() []output
}

// output: a channel a step writes to, and how to close it
type output struct {
	ch    any
	close func()
}

func outputOf[T any](ch chan<- T) output { return output{ch, func() { close(ch) }} }

// runtime: what a running step gets from its chain
type runtime struct {
	ctx   context.Context
	errch chan<- error
}

// Stopper: a step's logic may have one; it is called once, when the step ends
type Stopper interface{ Stop() }

// Flusher: a step's logic may hold values; when its input ends they go out
type Flusher[To any] interface {
	Flush() ([]To, error)
}

// ChainProcessor: steps that run together, one pass (Process); a chain is a step
// too (a sub-chain)
type ChainProcessor interface {
	Processor
	AddStep(p Processor)
	Process(ctx context.Context)
}

type chain struct {
	errch chan<- error
	steps []Processor
}

// NewChainProcessor: errch gets the steps' errors (nil: the errors of the chain this one runs in)
func NewChainProcessor(errch chan<- error) ChainProcessor {
	return &chain{errch: errch}
}

func (c *chain) AddStep(p Processor) {
	c.steps = append(c.steps, p)
}

// Process: one pass — every step runs until its input ends (or ctx does); returns
// when every step has
func (c *chain) Process(ctx context.Context) {
	c.run(runtime{ctx: ctx})
}

func (c *chain) outputs() []output { return nil }

func (c *chain) run(r runtime) {
	if c.errch != nil {
		r.errch = c.errch
	}
	// Every output closes once all its writers have returned
	writers := map[any]*sync.WaitGroup{}
	var outputs []output
	for _, s := range c.steps {
		for _, o := range s.outputs() {
			if writers[o.ch] == nil {
				writers[o.ch] = &sync.WaitGroup{}
				outputs = append(outputs, o)
			}
			writers[o.ch].Add(1)
		}
	}
	var all sync.WaitGroup
	for _, o := range outputs {
		all.Go(func() { writers[o.ch].Wait(); o.close() })
	}
	for _, s := range c.steps {
		all.Go(func() {
			defer func() {
				for _, o := range s.outputs() {
					writers[o.ch].Done()
				}
			}()
			s.run(r)
		})
	}
	all.Wait()
}

func (r runtime) report(err error) {
	if err == nil || errors.Is(err, ErrSkippedItem) || r.errch == nil {
		return
	}
	select {
	case r.errch <- err:
	case <-r.ctx.Done():
	}
}

func stop(logic any) {
	if s, ok := logic.(Stopper); ok {
		s.Stop()
	}
}

// runner: what every step but the entry point runs — values from in, each through
// the logic (each: the output's index and the value; -1: none), on n workers; when
// the input ends, after the last worker, what a single-output logic holds (Flusher)
type runner[Ti, To any] struct {
	n     int
	in    <-chan Ti
	outs  []chan<- To
	each  func(Ti) (int, To, error)
	logic any // Stopper, Flusher
}

func (s *runner[Ti, To]) outputs() []output {
	out := make([]output, len(s.outs))
	for i, o := range s.outs {
		out[i] = outputOf(o)
	}
	return out
}

func (s *runner[Ti, To]) run(r runtime) {
	defer stop(s.logic)
	var workers sync.WaitGroup
	for range max(s.n, 1) {
		workers.Go(func() {
			receive(r.ctx, s.in, func(v Ti) {
				if i, o, err := s.each(v); err != nil {
					r.report(err)
				} else if i >= 0 && i < len(s.outs) {
					send(r.ctx, s.outs[i], o)
				}
			})
		})
	}
	workers.Wait()
	if r.ctx.Err() == nil && len(s.outs) == 1 { // the input ended (not the pass)
		flushOut(r, s.logic, s.outs[0])
	}
}

// send: false if ctx ended first
func send[T any](ctx context.Context, ch chan<- T, v T) bool {
	select {
	case ch <- v:
		return true
	case <-ctx.Done():
		return false
	}
}

// receive reads in until it closes or ctx ends
func receive[T any](ctx context.Context, in <-chan T, each func(T)) {
	for {
		select {
		case <-ctx.Done():
			return
		case v, ok := <-in:
			if !ok {
				return
			}
			each(v)
		}
	}
}

// flushOut: what a Flusher holds, out before its output closes
func flushOut[To any](r runtime, logic any, out chan<- To) {
	f, ok := logic.(Flusher[To])
	if !ok {
		return
	}
	held, err := f.Flush()
	r.report(err)
	for _, v := range held {
		send(r.ctx, out, v)
	}
}
