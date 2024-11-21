package concurrency

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"testing"
)

func Test_GenStream(t *testing.T) {
	tests := []struct {
		name      string
		inputData []interface{}
		wantResp  []interface{}
		toCancel  bool
	}{
		{
			name:      "generates stream successfully",
			inputData: []interface{}{1, 2, 3},
			wantResp:  []interface{}{1, 2, 3},
			toCancel:  false,
		},
		{
			name:      "empty input",
			inputData: []interface{}{},
			wantResp:  []interface{}{},
			toCancel:  false,
		},
		{
			name:      "context canceled before processing",
			inputData: []interface{}{1, 2, 3},
			wantResp:  []interface{}{},
			toCancel:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			if tt.toCancel {
				cancel()
			}
			defer cancel()

			stream := GenStream(ctx, tt.inputData)

			var gotResp = make([]interface{}, 0)
			for v := range stream {
				gotResp = append(gotResp, v)
			}

			if !reflect.DeepEqual(tt.wantResp, gotResp) {
				t.Errorf("GenStream() got = %v, want = %v", gotResp, tt.wantResp)
			}
		})
	}
}

func Test_DoWork(t *testing.T) {
	tests := []struct {
		name      string
		inputData []interface{}
		workFunc  WorkFunc
		wantResp  []interface{}
		wantErr   bool
		toCancel  bool
	}{
		{
			name:      "processes all items successfully",
			inputData: []interface{}{1, 2, 3},
			workFunc: func(data interface{}) (interface{}, error) {
				return data.(int) * 2, nil
			},
			wantResp: []interface{}{2, 4, 6},
			wantErr:  false,
			toCancel: false,
		},
		{
			name:      "workFunc returns error for some items",
			inputData: []interface{}{1, 2, 3},
			workFunc: func(data interface{}) (interface{}, error) {
				if data.(int)%2 == 0 {
					return nil, fmt.Errorf("even numbers not allowed")
				}
				return data.(int) * 2, nil
			},
			wantResp: []interface{}{2, 6},
			wantErr:  false,
			toCancel: false,
		},
		{
			name:      "context canceled before processing",
			inputData: []interface{}{1, 2, 3},
			workFunc: func(data interface{}) (interface{}, error) {
				return data.(int) * 2, nil
			},
			wantResp: []interface{}{},
			wantErr:  false,
			toCancel: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			if tt.toCancel {
				cancel()
			}
			defer cancel()

			stream := GenStream(ctx, tt.inputData)
			output := DoWork(ctx, stream, tt.workFunc)

			var gotResp = make([]interface{}, 0)
			for v := range output {
				gotResp = append(gotResp, v)
			}

			if !reflect.DeepEqual(gotResp, tt.wantResp) {
				t.Errorf("DoWork() got = %v, want = %v", gotResp, tt.wantResp)
			}
		})
	}
}

func Test_ReadStream(t *testing.T) {
	tests := []struct {
		name     string
		input    []interface{}
		wantResp []interface{}
		toCancel bool
	}{
		{
			name:     "reads all items successfully",
			input:    []interface{}{1, 2, 3},
			wantResp: []interface{}{1, 2, 3},
			toCancel: false,
		},
		{
			name:     "empty input channel",
			input:    []interface{}{},
			wantResp: []interface{}{},
			toCancel: false,
		},
		{
			name:     "context canceled before completion",
			input:    []interface{}{1, 2, 3},
			wantResp: []interface{}{},
			toCancel: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			if tt.toCancel {
				cancel()
			}
			defer cancel()

			inputStream := GenStream(ctx, tt.input)
			outputStream := ReadStream(ctx, inputStream)

			var gotResp = make([]interface{}, 0)
			for v := range outputStream {
				gotResp = append(gotResp, v)
			}

			if !reflect.DeepEqual(gotResp, tt.wantResp) {
				t.Errorf("ReadStream() got = %v, want = %v", gotResp, tt.wantResp)
			}
		})
	}
}

func Test_FanOut(t *testing.T) {
	tests := []struct {
		name      string
		inputData []interface{}
		workFuncs []WorkFunc
		wantResp  [][]interface{}
		toCancel  bool
	}{
		{
			name:      "multiple workFuncs process items successfully",
			inputData: []interface{}{1, 2, 3},
			workFuncs: []WorkFunc{
				func(data interface{}) (interface{}, error) { return data.(int) + 1, nil },
				func(data interface{}) (interface{}, error) { return data.(int) * 2, nil },
			},
			wantResp: [][]interface{}{
				{2, 4, 6}, // First workFunc
				{2, 3, 4}, // Second workFunc
			},
			toCancel: false,
		},
		{
			name:      "workFunc returns error for some items",
			inputData: []interface{}{1, 2, 3},
			workFuncs: []WorkFunc{
				func(data interface{}) (interface{}, error) {
					if data.(int)%2 == 0 {
						return nil, fmt.Errorf("even numbers not allowed")
					}
					return data.(int) * 2, nil
				},
			},
			wantResp: [][]interface{}{
				{2, 6}, // Only odd numbers are processed
			},
			toCancel: false,
		},
		{
			name:      "context canceled before processing",
			inputData: []interface{}{1, 2, 3},
			workFuncs: []WorkFunc{
				func(data interface{}) (interface{}, error) { return data.(int) * 2, nil },
			},
			wantResp: [][]interface{}{{}},
			toCancel: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			if tt.toCancel {
				cancel()
			}
			defer cancel()
			inputStream := GenStream(ctx, tt.inputData)
			outputStreams := FanOut(ctx, inputStream, tt.workFuncs...)
			var gotResp [][]interface{}
			var wg sync.WaitGroup
			respCh := make(chan []interface{}, len(outputStreams))
			for _, stream := range outputStreams {
				wg.Add(1)
				go func(s <-chan interface{}) {
					defer wg.Done()
					var streamResp []interface{}
					for v := range s {
						streamResp = append(streamResp, v)
					}
					respCh <- streamResp
				}(stream)
			}
			go func() {
				wg.Wait()
				close(respCh)
			}()
			for resp := range respCh {
				gotResp = append(gotResp, resp)
			}
			// Sort the results to ensure deterministic comparison
			sort.Slice(gotResp, func(i, j int) bool {
				return len(gotResp[i]) > 0 && (gotResp[i][0].(int) < gotResp[j][0].(int))
			})
			// Normalize both gotResp and tt.wantResp for comparison
			normalize := func(input [][]interface{}) [][]interface{} {
				for i := range input {
					if input[i] == nil {
						input[i] = []interface{}{}
					}
				}
				return input
			}
			if !reflect.DeepEqual(normalize(gotResp), tt.wantResp) {
				t.Errorf("FanOut() got = %v, want = %v", gotResp, tt.wantResp)
			}
		})
	}
}

func Test_FanIn(t *testing.T) {
	tests := []struct {
		name      string
		inputData [][]interface{}
		wantResp  []interface{}
		toCancel  bool
	}{
		{
			name: "merges multiple streams",
			inputData: [][]interface{}{
				{1, 2, 3},
				{4, 5, 6},
			},
			wantResp: []interface{}{1, 2, 3, 4, 5, 6},
			toCancel: false,
		},
		{
			name: "empty streams",
			inputData: [][]interface{}{
				{},
				{},
			},
			wantResp: []interface{}{},
			toCancel: false,
		},
		{
			name: "context canceled before merging",
			inputData: [][]interface{}{
				{1, 2, 3},
			},
			wantResp: []interface{}{},
			toCancel: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			if tt.toCancel {
				cancel()
			}
			defer cancel()
			var streams []<-chan interface{}
			for _, data := range tt.inputData {
				streams = append(streams, GenStream(ctx, data))
			}
			outputStream := FanIn(ctx, streams)

			var gotResp = make([]interface{}, 0)
			for v := range outputStream {
				gotResp = append(gotResp, v)
			}
			sort.Slice(gotResp, func(i, j int) bool {
				return (gotResp[i].(int) < gotResp[j].(int))
			})
			if !reflect.DeepEqual(gotResp, tt.wantResp) {
				t.Errorf("FanIn() got = %v, want = %v", gotResp, tt.wantResp)
			}
		})
	}
}

func Test_DoBatchWork(t *testing.T) {
	tests := []struct {
		name          string
		inputData     []interface{}
		batchSize     int
		batchWorkFunc BatchWorkFunc
		wantResp      []interface{}
		toCancel      bool
	}{
		{
			name:      "processes all items in batches",
			inputData: []interface{}{1, 2, 3, 4, 5},
			batchSize: 2,
			batchWorkFunc: func(batch []interface{}) ([]interface{}, error) {
				var result []interface{}
				for _, item := range batch {
					result = append(result, item.(int)*2)
				}
				return result, nil
			},
			wantResp: []interface{}{2, 4, 6, 8, 10},
			toCancel: false,
		},
		{
			name:      "batchWorkFunc returns error for some batches",
			inputData: []interface{}{1, 2, 3, 4, 5},
			batchSize: 2,
			batchWorkFunc: func(batch []interface{}) ([]interface{}, error) {
				if len(batch) == 2 {
					return nil, fmt.Errorf("batch processing failed")
				}
				var result []interface{}
				for _, item := range batch {
					result = append(result, item.(int)*2)
				}
				return result, nil
			},
			wantResp: []interface{}{10},
			toCancel: false,
		},
		{
			name:      "context canceled before processing",
			inputData: []interface{}{1, 2, 3, 4, 5},
			batchSize: 2,
			batchWorkFunc: func(batch []interface{}) ([]interface{}, error) {
				var result []interface{}
				for _, item := range batch {
					result = append(result, item.(int)*2)
				}
				return result, nil
			},
			wantResp: []interface{}{},
			toCancel: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			if tt.toCancel {
				cancel()
			}
			defer cancel()

			inputStream := GenStream(ctx, tt.inputData)
			outputStream := DoBatchWork(ctx, inputStream, tt.batchSize, tt.batchWorkFunc)

			var gotResp = make([]interface{}, 0)
			for v := range outputStream {
				gotResp = append(gotResp, v)
			}

			if !reflect.DeepEqual(gotResp, tt.wantResp) {
				t.Errorf("DoBatchWork() got = %v, want = %v", gotResp, tt.wantResp)
			}
		})
	}
}
