package chain

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type mockEntryPoint[Ti any, To any] struct {
	startFunc    func(chan<- Ti, context.Context)
	decorateFunc func(Ti) (To, error)
	stopped      bool
	mu           sync.Mutex
}

func (m *mockEntryPoint[Ti, To]) Start(ch chan<- Ti, ctx context.Context) {
	if m.startFunc != nil {
		m.startFunc(ch, ctx)
	}
}

func (m *mockEntryPoint[Ti, To]) Decorate(input Ti) (To, error) {
	return m.decorateFunc(input)
}

func (m *mockEntryPoint[Ti, To]) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopped = true
}

func TestEntryPoint(t *testing.T) {
	t.Run("successful processing", func(t *testing.T) {
		chout := make(chan string)
		cherr := make(chan error, 1)

		mock := &mockEntryPoint[int, string]{
			startFunc: func(ch chan<- int, ctx context.Context) {
				ch <- 0 // will be converted to "A"
				ch <- 1 // will be converted to "B"
				ch <- 2 // will be converted to "C"
				close(ch)
			},
			decorateFunc: func(i int) (string, error) {
				return string(rune(i + 65)), nil // converts numbers to letters A, B, C...
			},
		}

		entry := NewEntryPoint(chout, mock)
		entry.setErrorChannel(cherr)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			entry.Process(ctx)
		}()

		results := make([]string, 0, 3)
		for i := 0; i < 3; i++ {
			select {
			case result := <-chout:
				results = append(results, result)
			case err := <-cherr:
				t.Errorf("unexpected error: %v", err)
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for result")
			}
		}

		wg.Wait()

		expected := map[string]bool{"A": true, "B": true, "C": true}
		if len(results) != len(expected) {
			t.Errorf("expected %d results, got %d", len(expected), len(results))
		}
		for _, res := range results {
			if !expected[res] {
				t.Errorf("unexpected result: %s", res)
			}
			delete(expected, res)
		}
		if len(expected) > 0 {
			t.Errorf("missing results: %v", expected)
		}
	})

	t.Run("error handling", func(t *testing.T) {
		chout := make(chan string)
		cherr := make(chan error, 1)
		expectedErr := errors.New("test error")

		mock := &mockEntryPoint[int, string]{
			startFunc: func(ch chan<- int, ctx context.Context) {
				ch <- 1
				close(ch)
			},
			decorateFunc: func(i int) (string, error) {
				return "", expectedErr
			},
		}

		entry := NewEntryPoint(chout, mock)
		entry.setErrorChannel(cherr)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			entry.Process(ctx)
		}()

		select {
		case err := <-cherr:
			if err != expectedErr {
				t.Errorf("expected error %v, got %v", expectedErr, err)
			}
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for error")
		}

		wg.Wait()
	})

	t.Run("stop on context cancel", func(t *testing.T) {
		chout := make(chan string)
		cherr := make(chan error, 1)

		mock := &mockEntryPoint[int, string]{
			startFunc: func(ch chan<- int, ctx context.Context) {
				<-ctx.Done() // Wait for cancellation
			},
			decorateFunc: func(i int) (string, error) {
				return string(rune(i + 65)), nil
			},
		}

		entry := NewEntryPoint(chout, mock)
		entry.setErrorChannel(cherr)

		ctx, cancel := context.WithCancel(context.Background())

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			entry.Process(ctx)
		}()

		cancel()
		wg.Wait()

		if !mock.stopped {
			t.Error("entry point was not stopped after context cancellation")
		}
	})
}
