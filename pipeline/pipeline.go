package pipeline

// ExecFn takes in a function as a parameter
// then performs the function and returns an
// outbound channel.
type ExecFn func(interface{}) (interface{}, error)

type IPipeline interface {
	Add(fn ExecFn) IPipeline
	Run() (<-chan interface{}, <-chan error)
}

type Pipeline struct {
	dataStream chan interface{}
	errors     chan error
	functions  []ExecFn
}

//
func run(in chan interface{}, fn ExecFn) (chan interface{}, chan error) {
	out := make(chan interface{})
	errors := make(chan error)

	go func() {
		// The out channel will close after all
		// the items from in channel have been
		// processed by the exec function.
		defer close(out)
		defer close(errors)
		for v := range in {
			result, err := fn(v)
			// For each item in the stream
			// we either get a result OR
			// an error so if an item in
			// the data stream errors out
			// it will NOT go onto the NEXT
			// function.
			if err != nil {
				errors <- err
				continue
			} else {
				out <- result
			}
		}
	}()
	return out, errors
}

// Run the functions in the pipeline.
func (p Pipeline) Run() (<-chan interface{}, <-chan error) {
	for i := 0; i < len(p.functions); i++ {
		p.dataStream, p.errors = run(p.dataStream, p.functions[i])
	}

	return p.dataStream, p.errors
}

// Add a function to run in the pipeline.
func (p Pipeline) Add(fn ExecFn) IPipeline {
	p.functions = append(p.functions, fn)
	return p
}

// New pipeline instance taking a generator function.
func New(f func(chan interface{})) IPipeline {
	in := make(chan interface{})

	go f(in)

	return &Pipeline{
		dataStream: in,
		errors:     make(chan error),
		functions:  []ExecFn{},
	}
}
