package jobqueue

import (
	"fmt"
	"github.com/itsabgr/ge"
	"testing"
)

func TestHandlerQueue(t *testing.T) {
	que := newHandlerQueue()

	ge.Throw(que.add("job1", func(job ImmutableJobQueue) error {
		fmt.Println("job 1 executed")
		return nil
	}))
	ge.Throw(que.add("job2", func(job ImmutableJobQueue) error {
		fmt.Println("job 2 executed")
		return nil
	}))
	ge.Throw(que.add("job3", func(job ImmutableJobQueue) error {
		fmt.Println("job 3 executed")
		return nil
	}))

	for id, hand := que.next(); hand != nil; id, hand = que.next() {
		fmt.Println("executing job : ", id, " | ", que.len())
		ge.Throw(hand(nil))
		ge.Throw(que.done(id))
	}

	ge.Assert(que.len() == 0)
}
