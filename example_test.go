package chain_test

import (
	"context"
	"fmt"
	"slices"
	"strings"

	chain "github.com/eggs-gd/go-chain"
)

// words: the chain's entry — every word of a text, then it is done
type words string

func (w words) Start(ctx context.Context, emit func(string) bool) error {
	for _, word := range strings.Fields(string(w)) {
		if !emit(word) {
			return ctx.Err()
		}
	}
	return nil
}

// length: a decorator — a word into its length (empty ones skipped, not an error)
type length struct{}

func (length) Decorate(w string) (int, error) {
	if w == "-" {
		return 0, chain.ErrSkippedItem
	}
	return len(w), nil
}

// parity: a switch — even lengths to output 0, odd ones to output 1
type parity struct{}

func (parity) Switch(n int) (int, error) { return n % 2, nil }

// collect: a chain's end
type collect struct{ got *[]int }

func (c collect) Consume(n int) error { *c.got = append(*c.got, n); return nil }

// A pass: words → lengths (on 4 workers) → split by parity → two ends. Process
// returns once every value went through every step.
func Example() {
	in, lengths := make(chan string), make(chan int)
	even, odd := make(chan int), make(chan int)
	var evens, odds []int

	c := chain.NewChainProcessor(nil)
	c.AddStep(chain.NewEntryPoint(in, words("a chain of - small steps")))
	c.AddStep(chain.NewDecoratorN(in, lengths, length{}, 4))
	c.AddStep(chain.NewSwitch(lengths, []chan<- int{even, odd}, parity{}))
	c.AddStep(chain.NewEnd(even, collect{&evens}))
	c.AddStep(chain.NewEnd(odd, collect{&odds}))
	c.Process(context.Background())

	slices.Sort(evens) // 4 workers: the order may change
	slices.Sort(odds)
	fmt.Println(evens, odds)
	// Output: [2] [1 5 5 5]
}
