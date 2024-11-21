package concurrency

import (
	"context"
	"sync"
)

type (
	WorkFunc      func(interface{}) (interface{}, error)
	BatchWorkFunc func([]interface{}) ([]interface{}, error)
)

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
				for i, workFunc := range workFuncs {
					wg.Add(1)
					go func(i int, data interface{}, workFunc WorkFunc) {
						defer wg.Done()
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
