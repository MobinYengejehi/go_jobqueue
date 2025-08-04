package jobqueue

import (
	"context"
	"sync"
	"testing"
)

// TestPausedIsThreadSafe ensures that calling Paused concurrently with
// Pause and Unpause does not cause data races.
func TestPausedIsThreadSafe(t *testing.T) {
	ctx, cancel := NewPausableContext(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			ctx.Pause()
			ctx.Unpause()
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			_ = ctx.Paused()
		}
	}()

	wg.Wait()
}
