package chain

// Decorator: a step's logic — one value in, one out (or an error). Optional: Stopper,
// Flusher.
type Decorator[Ti, To any] interface {
	Decorate(Ti) (To, error)
}

// NewDecorator: every value of in through the logic, to out
func NewDecorator[Ti, To any](in <-chan Ti, out chan<- To, logic Decorator[Ti, To]) Processor {
	return NewDecoratorN(in, out, logic, 1)
}

// NewDecoratorN: the same on n workers (the logic safe for concurrent use; the order
// may change)
func NewDecoratorN[Ti, To any](in <-chan Ti, out chan<- To, logic Decorator[Ti, To], n int) Processor {
	return &runner[Ti, To]{n: n, in: in, outs: []chan<- To{out}, logic: logic,
		each: func(v Ti) (int, To, error) { o, err := logic.Decorate(v); return 0, o, err }}
}
