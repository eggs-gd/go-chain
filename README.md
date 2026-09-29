# Chain Package

The `chain` package provides a flexible implementation of the Chain of Responsibility pattern for processing data through a series of steps.

## Overview

This package implements a pipeline processing system where each step in the chain can:
- Process data sequentially
- Transform data between different types
- Filter or skip items
- Branch processing paths based on conditions

## Core Components

### Interfaces

#### Processor
Base interface for all chain elements. Represents a processing unit that can be chained together:
```go
type Processor interface {
    // Process handles the main processing logic with context support
    Process(context.Context)
    // setErrorChannel configures error reporting channel
    setErrorChannel(chan<- error)
}
```

#### Decorator
Transforms input data to output data. Used for single-responsibility processors that modify or enrich data:
```go
type Decorator[Ti any, To any] interface {
    // Decorate transforms input type Ti to output type To
    Decorate(Ti) (To, error)
    // Stop handles cleanup when processing is done
    Stop()
}
```

#### EntryPoint
Starts the chain and provides initial data. Used as the first element in processing chains:
```go
type EntryPoint[Ti any, To any] interface {
    // Start initiates data feeding into the chain
    Start(chan<- Ti, context.Context)
    // Decorate transforms initial data if needed
    Decorate(Ti) (To, error)
    // Stop handles cleanup
    Stop()
}
```

#### Switcher
Branches processing paths based on input. Used when data needs to be routed to different processors:
```go
type Switcher[Ti any, To any] interface {
    // Switch decides which output channels should receive the data
    // Returns map[channelIndex]data
    Switch(Ti) (map[int]To, error)
    // Stop handles cleanup
    Stop()
}
```

### Implementation Types

#### ChainProcessor
Main implementation that manages sequential processing:
- Holds a sequence of processors
- Manages error propagation
- Handles context cancellation
- Ensures proper cleanup

#### DecoratorRunner
Generic implementation of the Decorator pattern:
- Handles channel communication
- Manages goroutines
- Provides error handling
- Ensures thread safety

#### EntryRunner
Implementation for chain entry points:
- Manages data ingestion
- Handles initial transformations
- Controls processing flow
- Provides cleanup mechanisms

#### SwitchRunner
Implementation for branching logic:
- Manages multiple output channels
- Routes data based on conditions
- Handles fan-out patterns
- Ensures proper channel management

## Usage Patterns

### Sequential Processing
```go
// Create a chain of processors that execute in order
chain := NewChainProcessor(errch)
chain.AddStep(validateData)
chain.AddStep(enrichData)
chain.AddStep(saveData)
```

### Transformation Pipeline
```go
// Create a pipeline that transforms data through multiple steps
decorator1 := NewDecorator(rawCh, parsedCh, parseStep)
decorator2 := NewDecorator(parsedCh, enrichedCh, enrichStep)
decorator3 := NewDecorator(enrichedCh, finalCh, finalizeStep)
```

### Branching Flow
```go
// Create a processor that routes data to different paths
switcher := NewSwitch(input, []chan<- Output{
    successPath,
    retryPath,
    errorPath,
}, routingLogic)
```

## Best Practices

1. Channel Management
   - Use buffered channels for errors
   - Close channels properly
   - Handle channel cleanup

2. Context Usage
   - Always pass context for cancellation
   - Implement proper cleanup on cancel
   - Use timeouts when appropriate

3. Error Handling
   - Use error channels for async errors
   - Handle all error cases
   - Provide meaningful error messages

4. Thread Safety
   - Ensure thread-safe state modifications
   - Use proper synchronization
   - Avoid race conditions

## Examples

See the `perceptors` directory for real-world examples:
- `exif_date` - EXIF date extraction using Decorator pattern
- `exif_geo` - Geolocation processing with data transformation
- `exif_size` - Image size processing showing chain usage
- `ml_color` - Color analysis demonstrating complex processing
```


