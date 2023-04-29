package pipeline

import (
	"fmt"
	"github.com/jkittell/toolbox"
	"testing"
)

var numbers = []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

var ifOdd = Stage[int](func(in <-chan int, out chan int) {
	for i := range in {
		n := i
		if n%2 == 1 {
			out <- n
		}
	}
})

var ifEven = Stage[int](func(in <-chan int, out chan int) {
	for i := range in {
		n := i
		if n%2 == 0 {
			out <- n
		}
	}
})

var double = Stage[int](func(in <-chan int, out chan int) {
	for i := range in {
		n := i * 2
		out <- n
	}
})

var square = Stage[int](func(in <-chan int, out chan int) {
	for i := range in {
		n := i * i
		out <- n
	}
})

var onSuccess = func(i int) {
	fmt.Println("sink running function on success ", i)
}

var onError = func(e error) {
	fmt.Println("sink running function on error ", e.Error())
}

func TestNewPipeline(t *testing.T) {
	done := make(chan bool)
	defer close(done)

	in := make(chan int)
	// for every odd, multiply times two, and add the results
	oddPipe := NewPipeline(ifOdd, double)
	out := oddPipe.Run(in)

	go func() {
		for i := 0; i < 10; i++ {
			in <- i
		}
		close(in)
	}()

	/*
		1 * 2 = 2
		3 * 2 = 6
		5 * 2 = 10
		7 * 2 = 14
		9 * 2 = 18
	*/
	exp := []int{2, 6, 10, 14, 18}
	var act []int
	for o := range out {
		act = append(act, o)
	}

	if !toolbox.Equals(exp, act) {
		t.Fail()
	}
}

func TestSource(t *testing.T) {
	done := make(chan bool)
	defer close(done)

	in := Source(done, numbers...)

	// for every odd, multiply times two, and add the results
	oddPipe := NewPipeline(ifOdd, double)
	out := oddPipe.Run(in)

	/*
		1 * 2 = 2
		3 * 2 = 6
		5 * 2 = 10
		7 * 2 = 14
		9 * 2 = 18
	*/
	exp := []int{2, 6, 10, 14, 18}
	var act []int
	for o := range out {
		act = append(act, o)
	}

	if !toolbox.Equals(exp, act) {
		t.Fail()
	}
}

func TestRepeat(t *testing.T) {
	done := make(chan bool)
	defer close(done)

	out := Take[int](done, Repeat[int](done, 1), 10)

	exp := []int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
	var act []int
	for o := range out {
		act = append(act, o)
	}

	if !toolbox.Equals(exp, act) {
		t.Fail()
	}
}

func TestTake(t *testing.T) {
	done := make(chan bool)
	defer close(done)

	in := Source(done, numbers...)

	out := Take[int](done, in, 3)

	exp := []int{0, 1, 2}
	var act []int
	for o := range out {
		act = append(act, o)
	}

	if !toolbox.Equals(exp, act) {
		t.Fail()
	}
}

func TestRepeatFn(t *testing.T) {
	done := make(chan bool)
	defer close(done)

	fn := func() int {
		return 1
	}

	out := Take[int](done, RepeatFn(done, fn), 10)

	exp := []int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
	var act []int
	for o := range out {
		act = append(act, o)
	}

	if !toolbox.Equals(exp, act) {
		t.Fail()
	}
}

func TestOrDone(t *testing.T) {
	done := make(chan bool)
	defer close(done)

	in := make(chan int)
	go func() {
		for i := 0; i < 100; i++ {
			if i < 10 {
				in <- i
			} else {
				close(in)
				return
			}
		}
		close(in)
	}()

	// for every odd, multiply times two
	oddPipe := NewPipeline(ifOdd, double)
	out := oddPipe.Run(in)

	exp := []int{2, 6, 10, 14, 18}
	var act []int
	for o := range OrDone[int](done, out) {
		act = append(act, o)
	}

	if !toolbox.Equals(exp, act) {
		t.Fail()
	}
}

func TestSplit(t *testing.T) {
	done := make(chan bool)
	defer close(done)

	in := Source(done, numbers...)
	out1, out2 := Split[int](done, in)

	for o := range out1 {
		v1 := o
		v2 := <-out2
		if !toolbox.Equals(v1, v2) {
			t.Fail()
		}
	}
}

func TestBridge(t *testing.T) {
	generator := func() <-chan <-chan int {
		chanStream := make(chan (<-chan int))
		go func() {
			defer close(chanStream)
			for i := 0; i < 10; i++ {
				stream := make(chan int, 1)
				stream <- i
				close(stream)
				chanStream <- stream
			}
		}()
		return chanStream
	}

	out := Bridge[int](nil, generator())

	exp := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	var act []int
	for o := range out {
		act = append(act, o)
	}

	if !toolbox.Equals(exp, act) {
		t.Fail()
	}
}

func TestFanOut(t *testing.T) {
	done := make(chan bool)
	defer close(done)

	in := Source(done, numbers...)

	out := FanOut[int](done, in, func(i int) int {
		return i
	}, 2)

	var act []int
	for o := range out {
		act = append(act, o)
	}

	if len(act) != 10 {
		t.Fail()
	}

	Sink[int](out, onSuccess)
}

func TestMerge(t *testing.T) {
	done := make(chan bool)
	defer close(done)

	in := Source(done, numbers...)

	out := FanOut[int](done, in, func(i int) int {
		return i
	}, 2)

	var act []int
	for o := range out {
		act = append(act, o)
	}

	if len(act) != 10 {
		t.Fail()
	}

	Sink[int](out, onSuccess)
}

func TestSink(t *testing.T) {
	done := make(chan bool)
	defer close(done)

	in := Source(done, numbers...)

	out := FanOut[int](done, in, func(i int) int {
		return i
	}, 2)

	Sink[int](out, onSuccess)
}
