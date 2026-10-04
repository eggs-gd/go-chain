package chain

// Switcher: a step's logic that picks one output for a value (its index). Optional:
// Stopper.
type Switcher[T any] interface {
	Switch(T) (int, error)
}

// SwitchDecorator: a switch's logic that also turns the value into another (the
// output's index and the new value). Optional: Stopper.
type SwitchDecorator[Ti, To any] interface {
	Switch(Ti) (int, To, error)
}

// NewSwitch: a value to one output; when the input ends, every output is done
func NewSwitch[T any](in <-chan T, outs []chan<- T, logic Switcher[T]) Processor {
	return NewSwitchN(in, outs, logic, 1)
}

// NewSwitchN: the same on n workers (the order may change)
func NewSwitchN[T any](in <-chan T, outs []chan<- T, logic Switcher[T], n int) Processor {
	return &runner[T, T]{n: n, in: in, outs: outs, logic: logic,
		each: func(v T) (int, T, error) { i, err := logic.Switch(v); return i, v, err }}
}

// NewSwitchDecorator: a value, turned into another, to one output
func NewSwitchDecorator[Ti, To any](in <-chan Ti, outs []chan<- To, logic SwitchDecorator[Ti, To]) Processor {
	return NewSwitchDecoratorN(in, outs, logic, 1)
}

// NewSwitchDecoratorN: the same on n workers (the order may change)
func NewSwitchDecoratorN[Ti, To any](in <-chan Ti, outs []chan<- To, logic SwitchDecorator[Ti, To], n int) Processor {
	return &runner[Ti, To]{n: n, in: in, outs: outs, logic: logic, each: logic.Switch}
}
