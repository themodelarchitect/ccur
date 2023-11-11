package ccur

type Semaphore struct {
	semCh chan struct{}
}

// NewSemaphore initiates a Semaphore by creating
// a buffered channel with the capacity of max.
func NewSemaphore(maxWorkers int) *Semaphore {
	return &Semaphore{semCh: make(chan struct{}, maxWorkers)}
}

// Acquire sends an empty struct to the semaphore channel.
// When the buffered semaphore channel is full, call to Acquire will
// be blocked.
func (s *Semaphore) Acquire() {
	s.semCh <- struct{}{}
}

// Release sends an empty struct out of the semaphore channel,
// creating space in the buffered semaphore channel for subsequent
// Acquire calls.
func (s *Semaphore) Release() {
	<-s.semCh
}
