# go-chain

[![Go Reference](https://pkg.go.dev/badge/github.com/eggs-gd/go-chain.svg)](https://pkg.go.dev/github.com/eggs-gd/go-chain)

A **Chain of Responsibility** for Go, built on channels: a value passes through a
chain of steps, each step does one thing to it — turns it into another, routes it,
drops it — and hands it on. Every step runs in its own goroutine; steps are
connected by typed channels; a chain may branch, join, run a step on several
workers and nest other chains. Generic, standard library only.

```
go get github.com/eggs-gd/go-chain
```

```go
in, lengths := make(chan string), make(chan int)
even, odd := make(chan int), make(chan int)

c := chain.NewChainProcessor(errs)                                     // every step's errors
c.AddStep(chain.NewEntryPoint(in, words(text)))                        // the one input
c.AddStep(chain.NewDecoratorN(in, lengths, length{}, 4))               // one step on 4 workers
c.AddStep(chain.NewSwitch(lengths, []chan<- int{even, odd}, parity{})) // a branch
c.AddStep(chain.NewEnd(even, collect{&evens}))
c.AddStep(chain.NewEnd(odd, collect{&odds}))
c.Process(ctx)                                                         // one pass, to its end
```

The whole example: [`example_test.go`](example_test.go).

## The pattern

Chain of Responsibility splits processing into handlers, each knowing one
responsibility and nothing of the others; a request travels the chain, each handler
deciding what to do with it. Here a handler is a **step**, and the chain is a
**pipeline**: many values travel at once, each step working on its own value
concurrently with the others. A step's logic is plain Go — an interface of one
method; the package runs it:

| logic | method | as a step |
|---|---|---|
| `EntryPoint[T]` | `Start(ctx, emit func(T) bool) error` | the chain's one input: emits its values, then is done |
| `Decorator[Ti, To]` | `Decorate(Ti) (To, error)` | one value in, one out |
| `Switcher[T]` | `Switch(T) (int, error)` | a value to one of several outputs (its index) |
| `SwitchDecorator[Ti, To]` | `Switch(Ti) (int, To, error)` | a value, turned into another, to one output |
| `Consumer[T]` | `Consume(T) error` | a chain's end: every value consumed |

Constructors connect a logic to channels: `NewEntryPoint(out, e)`,
`NewDecorator(in, out, d)`, `NewSwitch(in, outs, s)`, `NewSwitchDecorator(in,
outs, s)`, `NewEnd(in, c)`, and `NewChainProcessor(errs)` + `AddStep` for a chain.
Optional on any logic:

- **`Flusher[To]`** (`Flush() ([]To, error)`): what the step holds goes out when its
  input ends — a group not complete yet, a batch not full.
- **`Stopper`** (`Stop()`): called once, when the step ends — to close what it
  opened.

## A pass

`Process` is one pass. A chain has **one input, its entry point**: it emits its
values and returns, and its output closes. Every other step reads its input until
it closes, gives what it holds (`Flusher`) and returns. **An output closes once
every step that writes to it has returned** — the chain keeps a `sync.WaitGroup` per
output, one count per writer. So the end of the input travels the chain step by
step, and `Process` returns when every step has: by then every value of the pass
went through every step. No end-of-stream value in the data, no counting.

Channels close once: the next pass is a new chain with new channels.

## Branches and joins

```
               ┌─▶ even ─▶ [end]
[entry] ─▶ [switch]
               └─▶ odd ─▶ [decorator] ─┐
                                       ├─▶ joint ─▶ [end]
                           [another] ──┘
```

- **A switch** sends each value to one of its outputs; when its input ends, all its
  outputs close. A negative index drops the value on purpose; an index past the
  outputs is an error, not a value lost silently.
- **A join** is a channel several steps write to: it closes after the last of them
  returned, however long a branch takes.

## Workers

`NewDecoratorN`, `NewSwitchN`, `NewSwitchDecoratorN` run the same logic on `n`
goroutines reading one input (`n` last, as in `strings.SplitN`). The logic must be
safe for concurrent use; **the order of values may change**. The step returns after
its last worker: its output closes only when every value is done.

## Sub-chains

A chain is a step too: `AddStep(subchain)`. When it runs inside another chain, its
steps join the outer chain's: the writers of every channel are counted across all
levels, so a channel written by steps of two sub-chains closes after the last of
them. Made with `NewChainProcessor(nil)`, a sub-chain reports its errors to the
chain it runs in — a stage of a bigger pipeline declares its own steps in its own
constructor, and the top stays a list of stages.

## Errors and skips

- **A step's error** goes to the chain's error channel; the value is dropped, the
  pass goes on.
- **`ErrSkippedItem`** — a value dropped on purpose (buffered, unchanged, not
  wanted) — is **not an error**: it never reaches the error channel.
- An entry point's `Start` error is reported the same way.

## Cancellation

Every send — to a channel or to the error channel — gives up when the context
ends, so a cancelled pass never blocks on a full channel; `emit` returns false and
the entry point should return. A step stopped by the context does not flush.

## Tests

`go test -race ./...`: a pass, a flusher's values before its output closes, a switch
and a join that closes after its slow branch, a join across two sub-chains, a switch
index out of range, workers that finish every value, errors and skips, a sub-chain
inheriting the error channel, cancel unblocking a send, `Stop` called once; the
example.

## License

MIT
