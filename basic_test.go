package chain

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestDecorateAsync(t *testing.T) {
	t.Run("async processing", func(t *testing.T) {
		chin := make(chan int)
		chout := make(chan string)
		var wg sync.WaitGroup
		wg.Add(1)

		decorator := func(i int) (string, error) {
			return string(rune(i + 65)), nil
		}

		go DecorateAsync(2, chin, chout, decorator, &wg)

		go func() {
			for i := 0; i < 3; i++ {
				chin <- i
			}
			close(chin)
		}()

		results := make(map[string]bool)
		for result := range chout {
			results[result] = true
		}

		wg.Wait()

		expected := []string{"A", "B", "C"}
		if len(results) != len(expected) {
			t.Errorf("expected %d results, got %d", len(expected), len(results))
		}

		// Verify presence of all expected results
		for _, v := range expected {
			if !results[v] {
				t.Errorf("result %s not found in output data", v)
			}
		}
	})

	t.Run("error handling", func(t *testing.T) {
		chin := make(chan int)
		chout := make(chan string)
		var wg sync.WaitGroup
		wg.Add(1)

		decorator := func(i int) (string, error) {
			if i == 1 {
				return "", errors.New("test error")
			}
			return string(rune(i + 65)), nil
		}

		go DecorateAsync(2, chin, chout, decorator, &wg)

		go func() {
			for i := 0; i < 3; i++ {
				chin <- i
			}
			close(chin)
		}()

		results := make(map[string]bool)
		for result := range chout {
			results[result] = true
		}

		wg.Wait()

		expected := []string{"A", "C"}
		if len(results) != len(expected) {
			t.Errorf("expected %d results, got %d", len(expected), len(results))
		}

		// Verify presence of all expected results
		for _, v := range expected {
			if !results[v] {
				t.Errorf("result %s not found in output data", v)
			}
		}
	})
}

func TestDecorateWithInvalidWorkerCount(t *testing.T) {
	t.Run("zero worker count", func(t *testing.T) {
		chin := make(chan int)
		chout := make(chan string)

		decorator := func(i int) (string, error) {
			return string(rune(i + 65)), nil
		}

		go DecorateSync(0, chin, chout, decorator)

		// Verify channel is closed immediately
		select {
		case _, ok := <-chout:
			if ok {
				t.Error("channel should be closed for zero worker count")
			}
		case <-time.After(time.Second):
			t.Error("timeout waiting for channel to close")
		}
	})

	t.Run("negative worker count", func(t *testing.T) {
		chin := make(chan int)
		chout := make(chan string)

		decorator := func(i int) (string, error) {
			return string(rune(i + 65)), nil
		}

		go DecorateSync(-1, chin, chout, decorator)

		// Verify channel is closed immediately
		select {
		case _, ok := <-chout:
			if ok {
				t.Error("channel should be closed for negative worker count")
			}
		case <-time.After(time.Second):
			t.Error("timeout waiting for channel to close")
		}
	})
}

func TestDecorateWithNilFunction(t *testing.T) {
	chin := make(chan int)
	chout := make(chan string)
	var nilDecorator Decorate[int, string]

	go DecorateSync(1, chin, chout, nilDecorator)

	// Verify channel is closed immediately when nil function
	select {
	case _, ok := <-chout:
		if ok {
			t.Error("channel should be closed when nil function")
		}
	case <-time.After(time.Second):
		t.Error("timeout waiting for channel to close")
	}
}
