# Concurrency Package

A Go package that provides utility functions for managing concurrent data processing with context-based cancellation support. The package enables efficient data streaming, processing, and merging using worker functions.

## Features

- Stream data from a slice or channel.
- Perform concurrent data processing with worker functions.
- Merge and distribute data streams.
- Batch process data for optimized workloads.
- Context-based cancellation for graceful termination.

## Installation

To install the package, run:

```bash
go get github.com/cscookie/concurrency
```


## Getting started

Import the pacakge

```go
import "github.com/cscookie/concurrency"
```

### Example: Streaming and Processing Data

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/cscookie/concurrency"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	data := []interface{}{1, 2, 3, 4, 5}

	// Generate a data stream
	stream := concurrency.GenStream(ctx, data)

	// Define 2 simple WorkFuncs
	add := func(input interface{}) (interface{}, error) {
		num := input.(int)
		return num + 2, nil
	}
	subtract := func(input interface{}) (interface{}, error) {
		num := input.(int)
		return num - 1, nil
	}

	// Process the stream using the WorkFuncs
	addStream := concurrency.DoWork(ctx, stream, add)
	subtractStream := concurrency.DoWork(ctx, addStream, subtract)

	// Read and print the processed results
	for result := range concurrency.ReadStream(ctx, subtractStream) {
		fmt.Println(result)
	}
}
```
Output
```
2
3
4
5
6
```

### Example: Fan-Out and Fan-In

```go
package main

import (
	"context"
	"fmt"

	"github.com/cscookie/concurrency"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Example data
	data := []interface{}{1, 2, 3, 4, 5}

	// Generate a data stream
	stream := concurrency.GenStream(ctx, data)

	// Define multiple WorkFuncs
	workFunc1 := func(input interface{}) (interface{}, error) {
		num := input.(int)
		return num + 1, nil
	}
	workFunc2 := func(input interface{}) (interface{}, error) {
		num := input.(int)
		return num * 2, nil
	}

	// Fan out the data stream
	fannedOutStreams := concurrency.FanOut(ctx, stream, workFunc1, workFunc2)

	// Fan in the processed streams
	resultStream := concurrency.FanIn(ctx, fannedOutStreams)

	// Read and print the merged results
	for result := range resultStream {
		fmt.Println(result)
	}
}
```
Output
```
2
4
8
6
10
2
3
4
6
5
```

## Contributing
Contributions are welcome! Feel free to open an issue or submit a pull request.