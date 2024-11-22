package concurrency

import (
	"context"
	"sync"
)

type (
	// WorkFunc represents a function that processes a single piece of data and returns a result or an error.
	WorkFunc func(interface{}) (interface{}, error)

	// BatchWorkFunc represents a function that processes a batch of data and returns results or an error.
	BatchWorkFunc func([]interface{}) ([]interface{}, error)
)

// GenStream creates a channel that streams the elements of the given data slice.
// It stops streaming if the context is canceled.
//
// Parameters:
// - ctx: The context used to control cancellation.
// - data: The slice of data to stream.
//
// Returns:
// - A read-only channel that streams the data.
func GenStream(ctx context.Context, data []interface{}) <-chan interface{} {
	stream := make(chan interface{})
	go func() {
		defer close(stream)
		select {
		case <-ctx.Done():
			return
		default:
		}
		for _, d := range data {
			select {
			case <-ctx.Done():
				return
			case stream <- d:
			}
		}
	}()
	return stream
}

// DoWork processes data from an input channel using a given WorkFunc and streams the results.
// Processing stops if the context is canceled.
//
// Parameters:
// - ctx: The context used to control cancellation.
// - c: The input channel to read data from.
// - workFunc: The function to process each piece of data.
//
// Returns:
// - A read-only channel that streams the processed results.
func DoWork(ctx context.Context, c <-chan interface{}, workFunc WorkFunc) <-chan interface{} {
	stream := make(chan interface{})
	go func() {
		defer close(stream)
		for data := range c {
			select {
			case <-ctx.Done():
				return
			default:
				res, err := workFunc(data)
				if err != nil {
					continue
				}
				select {
				case <-ctx.Done():
					return
				case stream <- res:
				}
			}
		}
	}()
	return stream
}

// ReadStream reads data from an input channel and streams it to an output channel.
// Stops streaming if the context is canceled.
//
// Parameters:
// - ctx: The context used to control cancellation.
// - c: The input channel to read data from.
//
// Returns:
// - A read-only channel that streams the data.
func ReadStream(ctx context.Context, c <-chan interface{}) <-chan interface{} {
	stream := make(chan interface{})
	go func() {
		defer close(stream)
		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-c:
				if !ok {
					return
				}
				select {
				case <-ctx.Done():
					return
				case stream <- v:
				}
			}
		}
	}()
	return stream
}

// FanOut distributes data from a single input channel to multiple output channels,
// processing each item using a corresponding WorkFunc. Stops processing if the context is canceled.
//
// Parameters:
// - ctx: The context used to control cancellation.
// - c: The input channel to read data from.
// - workFuncs: A variadic list of WorkFunc functions for processing data.
//
// Returns:
// - A slice of read-only channels, each streaming the processed results.
func FanOut(ctx context.Context, c <-chan interface{}, workFuncs ...WorkFunc) []<-chan interface{} {
	streams := make([]chan interface{}, len(workFuncs))
	for i := range workFuncs {
		streams[i] = make(chan interface{})
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for data := range c {
			select {
			case <-ctx.Done():
				return
			default:
				innerWg := &sync.WaitGroup{}
				for i, workFunc := range workFuncs {
					innerWg.Add(1)
					go func(i int, data interface{}, workFunc WorkFunc) {
						defer innerWg.Done()
						res, err := workFunc(data)
						if err != nil {
							return
						}
						select {
						case <-ctx.Done():
							return
						case streams[i] <- res:
						}
					}(i, data, workFunc)
				}
				innerWg.Wait()
			}
		}
	}()
	go func() {
		wg.Wait()
		for _, s := range streams {
			close(s)
		}
	}()
	var readOnlyChan = func(c []chan interface{}) []<-chan interface{} {
		resChan := make([]<-chan interface{}, len(c))
		for i := range resChan {
			resChan[i] = c[i]
		}
		return resChan
	}
	return readOnlyChan(streams)
}

// FanIn merges multiple input channels into a single output channel.
// Stops merging if the context is canceled.
//
// Parameters:
// - ctx: The context used to control cancellation.
// - streams: A slice of read-only channels to merge.
//
// Returns:
// - A read-only channel streaming merged data from all input channels.
func FanIn(ctx context.Context, streams []<-chan interface{}) <-chan interface{} {
	stream := make(chan interface{})
	var wg sync.WaitGroup
	wg.Add(len(streams))
	for _, s := range streams {
		go func(s <-chan interface{}) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case res, ok := <-s:
					if !ok {
						return
					}
					select {
					case <-ctx.Done():
						return
					case stream <- res:
					}
				}
			}
		}(s)
	}
	go func() {
		wg.Wait()
		close(stream)
	}()
	return stream
}

// DoBatchWork processes data from an input channel in batches using a BatchWorkFunc.
// Streams the batch results to an output channel. Stops processing if the context is canceled.
//
// Parameters:
// - ctx: The context used to control cancellation.
// - c: The input channel to read data from.
// - batchSize: The number of items in each batch.
// - batchWorkFunc: The function to process a batch of data.
//
// Returns:
// - A read-only channel that streams the processed batch results.
func DoBatchWork(ctx context.Context, c <-chan interface{}, batchSize int, batchWorkFunc BatchWorkFunc) <-chan interface{} {
	stream := make(chan interface{})
	var batch []interface{}
	go func() {
		defer close(stream)
		for data := range c {
			select {
			case <-ctx.Done():
				return
			default:
				batch = append(batch, data)
				if len(batch) == batchSize {
					results, err := batchWorkFunc(batch)
					if err != nil {
						batch = nil
						continue
					}
					for _, res := range results {
						select {
						case <-ctx.Done():
							return
						case stream <- res:
						}
					}
					batch = nil
				}
			}
		}
		// Process remaining items in the batch after the input channel closes
		if len(batch) > 0 {
			results, err := batchWorkFunc(batch)
			if err != nil {
				return
			}
			for _, res := range results {
				select {
				case <-ctx.Done():
					return
				case stream <- res:
				}
			}
		}
	}()
	return stream
}
