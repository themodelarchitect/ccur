package pipeline

import (
	"fmt"
	"testing"
)

func TestPipeline_Add(t *testing.T) {

}

func TestPipeline_Run(t *testing.T) {
	out := New(func(in chan interface{}) {
		defer close(in)
		for i := 0; i < 5; i++ {
			in <- i
		}
	}).Add(func(in interface{}) (interface{}, error) {
		return in.(int) * 2, nil
	}).Add(func(in interface{}) (interface{}, error) {
		return in.(int) * in.(int), nil
	}).Add(func(in interface{}) (interface{}, error) {
		return fmt.Sprintf("%d\n", in.(int)), nil
	}).Add(func(in interface{}) (interface{}, error) {
		return in.(string), nil
	}).Run()

	for result := range out {
		fmt.Printf("Result: %s\n", result)
	}
}
