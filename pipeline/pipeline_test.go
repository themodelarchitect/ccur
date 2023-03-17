package pipeline

import (
	"errors"
	"fmt"
	"testing"
)

func print(i int) {
	fmt.Println("done - ", i)
}

func onErr(err error) {
	fmt.Println(err.Error())
}

func example(n int) {
	out, errors := Generate(func(in chan int) {
		defer close(in)
		for i := 0; i < n; i++ {
			fmt.Println("input", i)
			in <- i
		}
	}).Next(func(in any) (any, error) {
		return in, nil
	}).Next(func(in any) (any, error) {
		return in, nil
	}).Next(func(in any) (any, error) {
		message := fmt.Sprintf("error happend %d", in)
		return in, errors.New(message)
	}).Run()

	Sink(out, print, errors, onErr)
}

func TestPipeline_Run(t *testing.T) {
	for i := 0; i < 10000; i++ {
		example(i)
	}
}

func BenchmarkPipeline_Run(b *testing.B) {
	for i := 0; i < b.N; i++ {
		example(10000)
	}
}
