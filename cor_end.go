package chain

// Consumer: the logic of a chain's end — every value consumed. Optional: Stopper.
type Consumer[T any] interface {
	Consume(T) error
}

// NewEnd: a chain's end
func NewEnd[T any](in <-chan T, logic Consumer[T]) Processor {
	return &runner[T, struct{}]{n: 1, in: in, logic: logic,
		each: func(v T) (int, struct{}, error) { return -1, struct{}{}, logic.Consume(v) }}
}
