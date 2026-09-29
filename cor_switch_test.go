package chain

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type mockSwitcher[Ti any, To any] struct {
	switchFunc func(Ti) (map[int]To, error)
	stopped    bool
	mu         sync.Mutex
}

func (m *mockSwitcher[Ti, To]) Switch(input Ti) (map[int]To, error) {
	return m.switchFunc(input)
}

func (m *mockSwitcher[Ti, To]) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopped = true
}

func TestSwitch(t *testing.T) {
	t.Run("successful branching", func(t *testing.T) {
		chin := make(chan int)
		chout1 := make(chan string)
		chout2 := make(chan string)
		cherr := make(chan error, 1)

		mock := &mockSwitcher[int, string]{
			switchFunc: func(i int) (map[int]string, error) {
				if i%2 == 0 {
					return map[int]string{0: "even"}, nil
				}
				return map[int]string{1: "odd"}, nil
			},
		}

		sw := NewSwitch(chin, []chan<- string{chout1, chout2}, mock)
		sw.setErrorChannel(cherr)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			sw.Process(ctx)
		}()

		// Create channels for result synchronization
		results := make(chan string, 2)

		// Start goroutines for reading from output channels
		go func() {
			result := <-chout1
			results <- result
		}()
		go func() {
			result := <-chout2
			results <- result
		}()

		// Send test data
		chin <- 0 // should go to chout1
		chin <- 1 // should go to chout2
		close(chin)

		// Collect results
		var received []string
		for i := 0; i < 2; i++ {
			select {
			case result := <-results:
				received = append(received, result)
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for result")
			}
		}

		wg.Wait()

		// Verify results
		expected := map[string]bool{"even": true, "odd": true}
		for _, res := range received {
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
		chin := make(chan int)
		chout := []chan<- string{make(chan string), make(chan string)}
		cherr := make(chan error, 1)
		expectedErr := errors.New("test error")

		mock := &mockSwitcher[int, string]{
			switchFunc: func(i int) (map[int]string, error) {
				return nil, expectedErr
			},
		}

		sw := NewSwitch(chin, chout, mock)
		sw.setErrorChannel(cherr)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			sw.Process(ctx)
		}()

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
		chout := []chan<- string{make(chan string), make(chan string)}
		cherr := make(chan error, 1)

		mock := &mockSwitcher[int, string]{
			switchFunc: func(i int) (map[int]string, error) {
				return map[int]string{0: "test"}, nil
			},
		}

		sw := NewSwitch(chin, chout, mock)
		sw.setErrorChannel(cherr)

		ctx, cancel := context.WithCancel(context.Background())

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			sw.Process(ctx)
		}()

		cancel()
		wg.Wait()

		if !mock.stopped {
			t.Error("switch was not stopped after context cancellation")
		}
	})
}
