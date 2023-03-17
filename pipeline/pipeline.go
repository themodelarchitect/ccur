package pipeline

import (
	"sync"
)

// OnError will run when errors show up in the sink.
type OnError func(err error)

// ExecFn takes in a function as a parameter
// then performs the function and returns an
// outbound channel.
type ExecFn func(any) (any, error)

type Pipeline[T any] struct {
	dataStream chan T
	errors     chan error
	functions  []ExecFn
}

// Sink will take items from in channel and err channel and run the functions passed in as arguments.
func Sink[T any](in <-chan T, fn func(T), errors <-chan error, errorFn OnError) {
	for {
		select {
		case x, ok := <-in:
			if !ok {
				return // Done
			}
			fn(x)
		case err := <-errors:
			if err != nil {
				errorFn(err)
			}
		}
	}
}

// Merge or "fan in" joins together multiple streams of data into a single stream.
// For example multiple pipeline outputs would converge here using this function to
// send to the sink.
func Merge[T any](channels ...<-chan T) <-chan T {
	var wg sync.WaitGroup
	out := make(chan T)

	output := func(c <-chan T) {
		defer wg.Done()
		for n := range c {
			select {
			case out <- n:
			}
		}
	}

	wg.Add(len(channels))
	for _, c := range channels {
		go output(c)
	}

	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func run[T any](in <-chan T, fn ExecFn) (chan T, chan error) {
	// Create an output channel of type (T) and an error channel.
	outputChannel := make(chan T)
	errorChannel := make(chan error)

	go func() {
		defer close(outputChannel)
		defer close(errorChannel)

		for x := range in {
			out, err := fn(x)
			if err != nil {
				errorChannel <- err
				continue
			}
			o, ok := out.(T)
			if ok {
				outputChannel <- o
			}
		}
	}()
	return outputChannel, errorChannel
}

// Run the functions in the pipeline.
func (p *Pipeline[T]) Run() (<-chan T, <-chan error) {
	for i := 0; i < len(p.functions); i++ {
		p.dataStream, p.errors = run(p.dataStream, p.functions[i])
	}
	return p.dataStream, p.errors
}

// Next adds a function to run in the next stage of the pipeline.
func (p *Pipeline[T]) Next(fn ExecFn) *Pipeline[T] {
	p.functions = append(p.functions, fn)
	return p
}

// Generate takes in a function to generate a stream of items
// to run through the next steps in the pipeline.
func Generate[T any](f func(chan T)) *Pipeline[T] {
	in := make(chan T)
	errors := make(chan error)

	// generate the stream
	go f(in)
	return &Pipeline[T]{
		dataStream: in,
		errors:     errors,
		functions:  []ExecFn{},
	}
}
