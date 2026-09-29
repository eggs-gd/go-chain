package chain

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// MockDecorator implements Decorator interface for testing
type mockDecorator[Ti any, To any] struct {
	decorateFunc func(Ti) (To, error)
	stopped      bool
	mu           sync.Mutex
}

func (m *mockDecorator[Ti, To]) Decorate(input Ti) (To, error) {
	return m.decorateFunc(input)
}

func (m *mockDecorator[Ti, To]) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopped = true
}

func TestDecorator(t *testing.T) {
	t.Run("successful transformation", func(t *testing.T) {
		chin := make(chan int)
		chout := make(chan string)
		cherr := make(chan error, 1)

		mock := &mockDecorator[int, string]{
			decorateFunc: func(i int) (string, error) {
				return string(rune(i + 65)), nil // converts numbers to letters A, B, C...
			},
		}

		decorator := NewDecorator(chin, chout, mock)
		decorator.setErrorChannel(cherr)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			decorator.Process(ctx)
		}()

		// Send test data in a separate goroutine
		go func() {
			chin <- 0 // expect "A"
			chin <- 1 // expect "B"
			chin <- 2 // expect "C"
			close(chin)
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

		// Verify results
		expected := map[string]bool{"A": true, "B": true, "C": true}
		if len(results) != len(expected) {
			t.Errorf("expected %d results, got %d", len(expected), len(results))
		}
		for _, res := range results {
			if !expected[res] {
				t.Errorf("unexpected result: %s", res)
			}
		}
	})

	t.Run("error handling", func(t *testing.T) {
		chin := make(chan int)
		chout := make(chan string)
		cherr := make(chan error, 1)

		expectedErr := errors.New("test error")
		mock := &mockDecorator[int, string]{
			decorateFunc: func(i int) (string, error) {
				if i == 1 {
					return "", expectedErr
				}
				return string(rune(i + 65)), nil
			},
		}

		decorator := NewDecorator(chin, chout, mock)
		decorator.setErrorChannel(cherr)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			decorator.Process(ctx)
		}()

		// Send data in a separate goroutine
		go func() {
			chin <- 1
			close(chin)
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
		chin := make(chan int)
		chout := make(chan string)
		cherr := make(chan error, 1)

		mock := &mockDecorator[int, string]{
			decorateFunc: func(i int) (string, error) {
				return string(rune(i + 65)), nil
			},
		}

		decorator := NewDecorator(chin, chout, mock)
		decorator.setErrorChannel(cherr)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			decorator.Process(ctx)
		}()

		// Cancel context
		cancel()
		wg.Wait()

		if !mock.stopped {
			t.Error("decorator was not stopped after context cancellation")
		}
	})

	t.Run("close on channel close", func(t *testing.T) {
		chin := make(chan int)
		chout := make(chan string)
		cherr := make(chan error, 1)

		mock := &mockDecorator[int, string]{
			decorateFunc: func(i int) (string, error) {
				return string(rune(i + 65)), nil
			},
		}

		decorator := NewDecorator(chin, chout, mock)
		decorator.setErrorChannel(cherr)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			decorator.Process(ctx)
		}()

		// Close input channel
		close(chin)
		wg.Wait()

		// Verify that the decorator stopped
		if !mock.stopped {
			t.Error("decorator was not stopped after input channel closure")
		}
	})
}
