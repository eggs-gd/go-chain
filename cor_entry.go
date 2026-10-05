package chain

import "context"

// EntryPoint: the logic of a chain's entry — one pass: every value it finds, emitted
// (emit is false once the chain stops). Optional: Stopper.
type EntryPoint[T any] interface {
	Start(ctx context.Context, emit func(T) bool) error
}

type entryRunner[T any] struct {
	out   chan<- T
	logic EntryPoint[T]
}

// NewEntryPoint: the chain's one input, an output only; when it is done, its output closes
func NewEntryPoint[T any](out chan<- T, logic EntryPoint[T]) Processor {
	return &entryRunner[T]{out, logic}
}

func (e *entryRunner[T]) outputs() []output { return []output{outputOf(e.out)} }

func (e *entryRunner[T]) run(r runtime) {
	defer stop(e.logic)
	r.report(e.logic.Start(r.ctx, func(v T) bool { return send(r.ctx, e.out, v) }))
}
