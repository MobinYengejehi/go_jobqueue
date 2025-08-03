package jobqueue

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestPauseCtx(t *testing.T) {
	ctx, cancel := NewPausableContext(context.Background())
	defer cancel()

	ctx2, cancel2 := context.WithCancel(ctx)
	defer cancel2()

	go func() {
		time.Sleep(time.Second * 5)
		ctx.Pause()
		fmt.Println("context paused")
		time.Sleep(time.Second * 5)
		ctx.Unpause()
		fmt.Println("context unpasued")
		time.Sleep(time.Second * 3)
		cancel()
		fmt.Println("context canceled")
	}()

	go func() {
		i := 0
		for ; ctx.Err() == nil; i++ {
			ctx.WaitIfPaused()

			fmt.Println("i is : ", i)

			time.Sleep(time.Millisecond * 500)
		}
		fmt.Println("done first job")
	}()

	go func() {
		j := 0
		for ; ctx.Err() == nil; j += 15 {
			ctx.WaitIfPaused()

			fmt.Println("j is on top : ", j)

			time.Sleep(time.Millisecond * 100)
		}
		fmt.Println("done second job")
	}()

	<-ctx2.Done()
	fmt.Println("child context done")
	<-ctx.Done()
	fmt.Println("job done!")
}
