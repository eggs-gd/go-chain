package chain

import "sync"

// Decorate converts struct from Ti to To
type Decorate[Ti any, To any] func(Ti) (To, error)

func decorate[Ti any, To any](count int, chin <-chan Ti, chout chan<- To, fn Decorate[Ti, To], extWg *sync.WaitGroup) {
	if count <= 0 || fn == nil {
		close(chout)
		if extWg != nil {
			extWg.Done()
		}
		return
	}

	var intWg = sync.WaitGroup{}

	if extWg != nil {
		defer extWg.Done()
	}

	for i := 0; i < count; i++ {
		intWg.Add(1)
		go func() {
			defer intWg.Done()
			for input := range chin {
				res, err := fn(input)
				if err != nil {
					//Log? error channel?
					continue
				}
				chout <- res
			}
		}()
	}

	intWg.Wait()
	close(chout)
}

// DecorateSync Take raw data from first channel and put decarodated data to second.
// After input channel will be closed externally - complete job and close output channel also
//
// Parameters:
// - [Ti, To]: type in type out
// - count: amount of workers
// - chin, chout: channels from and to
// - fn: converter from in data to out data, takes input, returns converted
func DecorateSync[Ti any, To any](count int, chin <-chan Ti, chout chan<- To, fn Decorate[Ti, To]) {
	decorate(count, chin, chout, fn, nil)
}

// DecorateAsync Take raw data from first channel and put decarodated data to second.
// After input channel will be closed externally - complete job and close output channel also
//
// Parameters:
// - [Ti, To]: type in type out
// - count: amount of workers
// - chin, chout: channels from and to
// - fn: converter from in data to out data, takes input, returns converted
// - extWg: if runs as goroutine itself, provide external WaitGroup to proper handling of result
func DecorateAsync[Ti any, To any](count int, chin <-chan Ti, chout chan<- To, fn Decorate[Ti, To], extWg *sync.WaitGroup) {
	decorate(count, chin, chout, fn, extWg)
}
