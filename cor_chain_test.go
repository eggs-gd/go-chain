package chain

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// MockProcessor implements Processor interface for testing
type MockProcessor struct {
	processFunc func(context.Context)
	errch       chan<- error
	stopped     bool
	mu          sync.Mutex
}

func (m *MockProcessor) Process(ctx context.Context) {
	if m.processFunc != nil {
		m.processFunc(ctx)
	}
}

func (m *MockProcessor) setErrorChannel(errch chan<- error) {
	m.errch = errch
}

func (m *MockProcessor) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopped = true
}

func TestChainProcessor(t *testing.T) {
	t.Run("sequential step execution", func(t *testing.T) {
		errch := make(chan error, 1)
		chain := NewChainProcessor(errch)

		executionOrder := make([]int, 0)
		var mu sync.Mutex

		// Create three processors that record their execution order
		for i := 0; i < 3; i++ {
			i := i // Capture variable for closure
			processor := &MockProcessor{
				processFunc: func(ctx context.Context) {
					mu.Lock()
					executionOrder = append(executionOrder, i)
					mu.Unlock()
					time.Sleep(10 * time.Millisecond) // Simulate work
				},
			}
			chain.AddStep(processor)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		chain.Process(ctx)

		if len(executionOrder) != 3 {
			t.Errorf("expected 3 steps, got %d", len(executionOrder))
		}
	})

	t.Run("error handling", func(t *testing.T) {
		errch := make(chan error, 1)
		chain := NewChainProcessor(errch)

		expectedErr := errors.New("test error")
		processor := &MockProcessor{
			processFunc: func(ctx context.Context) {
				errch <- expectedErr
			},
		}

		chain.AddStep(processor)

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		go chain.Process(ctx)

		select {
		case err := <-errch:
			if err != expectedErr {
				t.Errorf("expected error %v, got %v", expectedErr, err)
			}
		case <-time.After(200 * time.Millisecond):
			t.Error("timeout waiting for error")
		}
	})

	t.Run("cancel context", func(t *testing.T) {
		errch := make(chan error, 1)
		chain := NewChainProcessor(errch)

		processorDone := make(chan struct{})
		processor := &MockProcessor{
			processFunc: func(ctx context.Context) {
				<-ctx.Done()
				close(processorDone)
			},
		}
		chain.AddStep(processor)

		ctx, cancel := context.WithCancel(context.Background())

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			chain.Process(ctx)
		}()

		// Cancel context after short delay
		time.Sleep(50 * time.Millisecond)
		cancel()

		// Check that processor received cancellation signal
		select {
		case <-processorDone:
			// Success
		case <-time.After(100 * time.Millisecond):
			t.Error("processor did not receive cancellation signal")
		}

		wg.Wait()
	})

	t.Run("add steps", func(t *testing.T) {
		errch := make(chan error, 1)
		ch := NewChainProcessor(errch)

		processors := make([]*MockProcessor, 3)
		for i := range processors {
			processors[i] = &MockProcessor{}
			ch.AddStep(processors[i])
		}

		// Check number of processors directly
		if len(ch.actors) != 3 {
			t.Errorf("expected 3 processors, got %d", len(ch.actors))
		}

		// Check that error channel is set for all processors
		for i, p := range processors {
			if p.errch != errch {
				t.Errorf("processor %d: error channel not set", i)
			}
		}
	})
}

func TestErrSkippedItem(t *testing.T) {
	err := ErrSkippedItem
	if err.Error() == "" {
		t.Error("ErrSkippedItem should have an error message")
	}
}
