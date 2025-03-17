package ccur

import (
	"github.com/themodelarchitect/data/structures"
	"sync"
)

/*
Code adapted from examples in this book...
Concurrency in Go: Tools and Techniques for Developers 1st Edition
by Katherine Cox-Buday (Author)

https://go.dev/blog/pipelines
There’s no formal definition of a pipeline in Go; it’s just one of many kinds of concurrent programs.
Informally, a pipeline is a series of stages connected by channels,
where each stage is a group of goroutines running the same function.

Each stage has any number of inbound and outbound channels, except the first and last stages,
which have only outbound or inbound channels, respectively.

The first stage is sometimes called the source or producer; the last stage, the sink or consumer.
*/

// Stage takes an input channel of type T, an output channel of type T.
// The output channel is the input channel for the next ExecFn in the pipeline.
type Stage[T any] func(<-chan T, chan T)

// Run receive values from upstream via inbound channel.
// Performs some function on that data, usually producing new values.
// Send values downstream via outbound channel
func (s Stage[T]) Run(in <-chan T) chan T {
	out := make(chan T)
	go func() {
		s(in, out)
		close(out)
	}()
	return out
}

// Pipeline is an array of functions to run in sequence.
type Pipeline[T any] struct {
	Stages *structures.Array[Stage[T]]
}

// Run the functions in the pipeline.
func (p Pipeline[T]) Run(in <-chan T) <-chan T {
	for i := 0; i < p.Stages.Length(); i++ {
		stage := p.Stages.Lookup(i)
		in = stage.Run(in)
	}
	return in
}

func NewPipeline[T any](stages *structures.Array[Stage[T]]) Pipeline[T] {
	return Pipeline[T]{Stages: stages}
}

// Sink will take items from a channel of type T and run the function on each item.
func Sink[T any](in <-chan T, fn func(T)) {
	// loop until the in channel is closed
	for {
		select {
		case x, ok := <-in:
			if !ok { // in channel is closed
				return // Done
			}
			fn(x)
		}
	}
}

// Merge or "fan in" joins together multiple streams of data into a single stream.
// For example multiple pipeline outputs would converge here using this function to
// send to the sink. Fanning in means multiplexing or joining together multiple
// streams of data into a single stream. Merge takes in a done channel to allow the
// goroutines to be torn down, and then a variadic slice of type T channels to fan-in.
func Merge[T any](done <-chan bool, channels ...<-chan T) <-chan T {
	// Create a wait group so that we can wait until all channels have been drained.
	var wg sync.WaitGroup
	multiplexedStream := make(chan T)

	// When passed a channel this multiplex function will read from the
	// channel, and pass the value read on the multiplexedStream channel.
	multiplex := func(c <-chan T) {
		defer wg.Done()
		for x := range c {
			select {
			case <-done:
				return
			case multiplexedStream <- x:
			}
		}
	}

	// Select from all the channels
	// This increments the wait group by the number of channels we're multiplexing.
	wg.Add(len(channels))
	for _, c := range channels {
		go multiplex(c)
	}

	// Wait for all the reads to complete
	// Wait for all the channels we're multiplexing to be drained so that we can
	// close the multiplexedStream channel.
	go func() {
		wg.Wait()
		close(multiplexedStream)
	}()
	return multiplexedStream
}

// worker function that spawns a new goroutine to
// read data from an input channel and then perform
// a function on the data, and finally send the
// transformed data on an output channel.
// The output channel will close as soon
// as the input channel does (see the deferred close).
// Closing the output channel will signal downstream that
// all the data has been processed.
func worker[T any](in <-chan T, fn func(T) T) <-chan T {
	out := make(chan T)
	go func() {
		defer close(out)
		for i := range in {
			out <- fn(i)
		}
	}()
	return out
}

// FanOut is a group of goroutines to handle a pipeline stage.
// Multiple functions can read from the same channel until that channel is closed; this is called fan-out.
// This provides a way to distribute work amongst a group of workers to parallelize CPU use and I/O.
// What makes a pipeline stage suited for fanning out?
// Consider fanning out one of your stages if both of the following apply:
// 1. It doesn't rely on values that the stage had calculated before.
// 2. It takes a long time to run.
// The property of order-independence is important because you have no guarantee in
// what order concurrent copies of your stage will run, nor in what order they will return.
func FanOut[T any](done <-chan bool, in <-chan T, fn func(T) T, numWorkers int) <-chan T {
	// spawn a fixed number of fn(T) T worker goroutines
	// all reading from the same input channel.
	// workerOuts is a slice of channels as work is being
	// distributed from one goroutine to many.
	workerOuts := make([]<-chan T, numWorkers)
	for i := 0; i < numWorkers; i++ {
		workerOuts[i] = worker[T](in, fn)
	}

	// Merge will Fan-In the channels from all the
	// workers into one channel.
	return Merge[T](done, workerOuts...)
}

// Bridge returns a single-channel facade over a channel of channels.
func Bridge[T any](done <-chan bool, chanStream <-chan <-chan T) <-chan T {
	// This is the channel that will return all values.
	valueStream := make(chan T)
	go func() {
		defer close(valueStream)
		// This loop is responsible for pulling channels off of channelStream
		// and providing them to a nested loop for use.
		for {
			var stream <-chan T
			select {
			case maybeStream, ok := <-chanStream:
				if ok == false {
					return
				}
				stream = maybeStream
			case <-done:
				return
			}
			// This loop is responsible for reading values off the channel it has
			// been given and repeating those values onto valueStream. When the
			// stream we're currently looping over is closed, we break out of the
			// loop performing the reads from this channel, and continue with the next
			// iteration of the loop, selecting channels to read from. This provides us with
			// an unbroken stream of values.
			for value := range OrDone(done, stream) {
				select {
				case valueStream <- value:
				case <-done:
				}
			}
		}
	}()
	return valueStream
}

// Split takes in a channel to read from, and it will return two separate
// channels that will get the same value.
func Split[T any](done <-chan bool, in <-chan T) (<-chan T, <-chan T) {
	out1 := make(chan T)
	out2 := make(chan T)
	go func() {
		defer close(out1)
		defer close(out2)
		for v := range OrDone[T](done, in) {
			// Use local versions of out1 and out2 to shadow these variables.
			var out1, out2 = out1, out2
			// Use one select statement so that writes to out1 and out2 don't
			// block each other. To ensure both are written to, we'll perform
			// two iterations of the select statement: one for each outbound channel.
			for i := 0; i < 2; i++ {
				select {
				case <-done:
				case out1 <- v:
					// Once we've written to a channel, we set its shadowed copy to nil
					// so that further writes will block and the other channel may continue
					out1 = nil
				case out2 <- v:
					// Once we've written to a channel, we set its shadowed copy to nil
					// so that further writes will block and the other channel may continue
					out2 = nil
				}
			}
		}
	}()
	return out1, out2
}

// OrDone will read from the channel until it gets a done signal or the upstream
// channel is closed.
func OrDone[T any](done <-chan bool, stream <-chan T) <-chan T {
	valueStream := make(chan T)
	go func() {
		defer close(valueStream)
		for {
			select {
			case <-done:
				return
			case v, ok := <-stream:
				if ok == false {
					return
				}
				select {
				case valueStream <- v:
				case <-done:
				}
			}
		}
	}()
	return valueStream
}

// RepeatFn is a generator that repeatedly calls a function.
func RepeatFn[T any](done <-chan bool, fn func() T) <-chan T {
	stream := make(chan T)
	go func() {
		defer close(stream)
		for {
			select {
			case <-done:
				return
			case stream <- fn():
			}
		}
	}()
	return stream
}

// Take will only take n number of items off of the incoming stream and then exit.
func Take[T any](done <-chan bool, stream <-chan T, n int) <-chan T {
	takeStream := make(chan T)
	go func() {
		defer close(takeStream)
		for i := 0; i < n; i++ {
			select {
			case <-done:
				return
			case takeStream <- <-stream:
			}
		}
	}()
	return takeStream
}

// Repeat will repeat the values you pass to it infinitely until you tell it to stop.
func Repeat[T any](done <-chan bool, values ...T) <-chan T {
	stream := make(chan T)
	go func() {
		defer close(stream)
		for {
			for _, v := range values {
				select {
				case <-done:
					return
				case stream <- v:
				}
			}
		}
	}()
	return stream
}

// Source takes in an array of type T, constructs a buffered
// channel of T with a length equal to the incoming T slice, starts a
// goroutine, and returns the constructed channel. Then, on the goroutine that
// was created, Source ranges over the variadic slice that was passed in and sends
// the slices' values on the channel it created.
func Source[T any](done <-chan bool, values *structures.Array[T]) chan T {
	stream := make(chan T)
	go func() {
		defer close(stream)
		for i := 0; i < values.Length(); i++ {
			select {
			case <-done:
				return
			case stream <- values.Lookup(i):
			}
		}
	}()
	return stream
}
