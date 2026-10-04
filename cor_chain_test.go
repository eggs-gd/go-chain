package chain

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fn[Ti, To any] func(Ti) (To, error)

func (f fn[Ti, To]) Decorate(v Ti) (To, error) { return f(v) }

// collect: the values an end consumed
type collect[T any] struct {
	mu  sync.Mutex
	got []T
}

func (c *collect[T]) Consume(v T) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.got = append(c.got, v)
	return nil
}

// values: a pass emits these
type values []int

func (vs values) Start(_ context.Context, emit func(int) bool) error {
	for _, v := range vs {
		emit(v)
	}
	return nil
}

// pass: one pass of c, bounded
func pass(t *testing.T, c ChainProcessor) {
	t.Helper()
	done := make(chan struct{})
	go func() { c.Process(t.Context()); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the pass did not end")
	}
}

var same = fn[int, int](func(v int) (int, error) { return v, nil })

// A pass: the entry's values through every step; Process returns when the input
// ended everywhere — every value went through by then
func TestPass(t *testing.T) {
	in, out := make(chan int), make(chan int)
	got := &collect[int]{}
	c := NewChainProcessor(nil)
	c.AddStep(NewEntryPoint(in, values{1, 2, 3}))
	c.AddStep(NewDecorator(in, out, fn[int, int](func(v int) (int, error) { return v * 10, nil })))
	c.AddStep(NewEnd(out, got))
	pass(t, c)
	if !slices.Equal(got.got, []int{10, 20, 30}) {
		t.Errorf("got %v", got.got)
	}
}

// holder keeps every value until its input ends
type holder struct{ held []int }

func (h *holder) Decorate(v int) (int, error) { h.held = append(h.held, v); return 0, ErrSkippedItem }
func (h *holder) Flush() ([]int, error)       { return h.held, nil }

// A Flusher's values go out when its input ends, before its output closes
func TestFlusher(t *testing.T) {
	in, out := make(chan int), make(chan int)
	got := &collect[int]{}
	c := NewChainProcessor(nil)
	c.AddStep(NewEntryPoint(in, values{1, 2}))
	c.AddStep(NewDecorator[int, int](in, out, &holder{}))
	c.AddStep(NewEnd(out, got))
	pass(t, c)
	if !slices.Equal(got.got, []int{1, 2}) {
		t.Errorf("got %v", got.got)
	}
}

type parity struct{}

func (parity) Switch(v int) (int, error) { return v % 2, nil }

// NewSwitch: a value to one output; the branches join: the joint closes only after its
// last writer returned (a slow branch's values are not lost); a Flusher after the
// join gets everything
func TestRouteAndJoin(t *testing.T) {
	in, even, odd := make(chan int), make(chan int), make(chan int)
	joined, tail := make(chan int), make(chan int)
	slow := fn[int, int](func(v int) (int, error) { time.Sleep(10 * time.Millisecond); return v, nil })
	got := &collect[int]{}
	c := NewChainProcessor(nil)
	c.AddStep(NewEntryPoint(in, values{1, 2, 3, 4}))
	c.AddStep(NewSwitch(in, []chan<- int{even, odd}, parity{}))
	c.AddStep(NewDecorator(even, joined, slow))
	c.AddStep(NewDecorator(odd, joined, same))
	c.AddStep(NewDecorator[int, int](joined, tail, &holder{}))
	c.AddStep(NewEnd(tail, got))
	pass(t, c)
	if slices.Sort(got.got); !slices.Equal(got.got, []int{1, 2, 3, 4}) {
		t.Errorf("got %v", got.got)
	}
}

// NewDecoratorN: every value done before its output closes
func TestDecoratorN(t *testing.T) {
	in, out := make(chan int), make(chan int)
	got := &collect[int]{}
	var busy atomic.Int32
	c := NewChainProcessor(nil)
	c.AddStep(NewEntryPoint(in, values{1, 2, 3, 4, 5, 6}))
	c.AddStep(NewDecoratorN(in, out, fn[int, int](func(v int) (int, error) {
		busy.Add(1)
		defer busy.Add(-1)
		time.Sleep(time.Duration(7-v) * 3 * time.Millisecond)
		return v, nil
	}), 3))
	c.AddStep(NewEnd(out, got))
	pass(t, c)
	if len(got.got) != 6 || busy.Load() != 0 {
		t.Errorf("got %v, busy %d", got.got, busy.Load())
	}
}

// A skip is never an error; an error is; a sub-chain uses its parent's channel
func TestErrors(t *testing.T) {
	errch := make(chan error, 10)
	in, out := make(chan int), make(chan int)
	boom := errors.New("boom")
	sub := NewChainProcessor(nil)
	sub.AddStep(NewEntryPoint(in, values{1, 2, 3}))
	sub.AddStep(NewDecorator(in, out, fn[int, int](func(v int) (int, error) {
		switch v {
		case 1:
			return 0, ErrSkippedItem
		case 2:
			return 0, boom
		}
		return v, nil
	})))
	got := &collect[int]{}
	c := NewChainProcessor(errch)
	c.AddStep(sub)
	c.AddStep(NewEnd(out, got))
	pass(t, c)
	if len(errch) != 1 || !errors.Is(<-errch, boom) || !slices.Equal(got.got, []int{3}) {
		t.Errorf("errors %d, got %v", len(errch), got.got)
	}
}

type stopCount struct{ n *atomic.Int32 }

func (s stopCount) Decorate(v int) (int, error) { return v, nil }
func (s stopCount) Stop()                       { s.n.Add(1) }

// Cancel ends a step blocked on a send nobody reads; its logic is stopped once
func TestCancel(t *testing.T) {
	in, out := make(chan int), make(chan int) // nobody reads out
	var stops atomic.Int32
	c := NewChainProcessor(nil)
	c.AddStep(NewEntryPoint(in, values{1}))
	c.AddStep(NewDecorator[int, int](in, out, stopCount{&stops}))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { c.Process(ctx); close(done) }()
	time.Sleep(20 * time.Millisecond) // the value is stuck in the step
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the chain did not stop")
	}
	if stops.Load() != 1 {
		t.Errorf("Stop called %d times", stops.Load())
	}
}

// label: a switch that turns an int into its text, odd and even apart
type label struct{}

func (label) Switch(v int) (int, string, error) { return v % 2, strconv.Itoa(v), nil }

// NewSwitchDecoratorN: a value turned into another, to one output, on n workers
func TestSwitchDecoratorN(t *testing.T) {
	in, even, odd := make(chan int), make(chan string), make(chan string)
	evens, odds := &collect[string]{}, &collect[string]{}
	c := NewChainProcessor(nil)
	c.AddStep(NewEntryPoint(in, values{1, 2, 3, 4, 5}))
	c.AddStep(NewSwitchDecoratorN(in, []chan<- string{even, odd}, label{}, 3))
	c.AddStep(NewEnd(even, evens))
	c.AddStep(NewEnd(odd, odds))
	pass(t, c)
	slices.Sort(evens.got)
	slices.Sort(odds.got)
	if !slices.Equal(evens.got, []string{"2", "4"}) || !slices.Equal(odds.got, []string{"1", "3", "5"}) {
		t.Errorf("even %v, odd %v", evens.got, odds.got)
	}
}
