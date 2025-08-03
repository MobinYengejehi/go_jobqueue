package jobqueue

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/itsabgr/ge"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"strconv"
	"testing"
	"time"
)

func TestJobQueue(t *testing.T) {
	queue := NewJobQueue(context.Background(), "queue-1", nil)

	ge.Throw(queue.Add("w_on_i", func(job ImmutableJobQueue) error {
		fmt.Println("starting queue : ", job.WorkerID(), " | ", job.JobID())

		i := 0
		for ; job.Running() && i < 15; i++ {
			fmt.Println("processing i : ", i)
			time.Sleep(time.Millisecond * 500)
		}

		if i < 10 {
			return errors.New("i couldn't reach to 10: i = " + strconv.Itoa(i))
		}

		return nil
	}))
	ge.Throw(queue.Add("w_on_j", func(job ImmutableJobQueue) error {
		fmt.Println("starting queue : ", job.WorkerID(), " | ", job.JobID())

		j := 15
		for ; job.Running() && j < 1050; j += j % 150 {
			fmt.Println("processing j : ", j)
			time.Sleep(time.Millisecond * 100)
		}

		if j < 1000 {
			return errors.New("j couldn't reach to 1000: j = " + strconv.Itoa(j))
		}

		return nil
	}))

	go func() {
		time.Sleep(time.Second * 5)
		queue.Pause()
		fmt.Println("queue paused")
		time.Sleep(time.Second * 5)
		queue.Unpause()
		fmt.Println("queue unpaused")
		time.Sleep(time.Second * 10)
		queue.Cancel()
		fmt.Println("queue canceled")
	}()

	ge.Throw(queue.Process())
}

func TestJobQueueDatabase(t *testing.T) {
	ldb := ge.Must(leveldb.Open(storage.NewMemStorage(), nil))
	defer ldb.Close()
	db := NewJobLevelDBWrapper(ldb)

	queue := NewJobQueue(context.Background(), "queue-1", db)

	terminate := func(job JobQueue, pt time.Duration, ct time.Duration) {
		time.Sleep(time.Second * pt)
		job.Pause()
		fmt.Println("queue paused")
		time.Sleep(time.Second * pt)
		job.Unpause()
		fmt.Println("queue unpaused")
		time.Sleep(time.Second * ct)
		job.Cancel()
		fmt.Println("queue canceled")
	}

	woni := func(job ImmutableJobQueue) error {
		fmt.Println("starting queue : ", job.WorkerID(), " | ", job.JobID())

		key := []byte("i")
		i := uint64(0)
		var iBufW [8]byte
		if ge.Must(job.HasData(key)) {
			iBuf := ge.Must(job.GetData(key))
			if iBuf != nil {
				i = binary.LittleEndian.Uint64(iBuf)
			}
		}
		for ; job.Running() && i < 15; i++ {
			fmt.Println("i is : ", i)
			binary.LittleEndian.PutUint64(iBufW[:], i)
			ge.Throw(job.SetData(key, iBufW[:]))

			time.Sleep(time.Millisecond * 500)
		}

		if i < 10 {
			return errors.New("i couldn't reach to 10; i = " + strconv.Itoa(int(i)))
		}

		return nil
	}
	wonj := func(job ImmutableJobQueue) error {
		fmt.Println("starting queue : ", job.WorkerID(), " | ", job.JobID())

		key := []byte("j")
		j := uint64(15)
		var iBufW [8]byte
		if ge.Must(job.HasData(key)) {
			iBuf := ge.Must(job.GetData(key))
			if iBuf != nil {
				j = binary.LittleEndian.Uint64(iBuf)
			}
		}
		for ; job.Running() && j < 1050; j += j % 150 {
			fmt.Println("j is : ", j)
			binary.LittleEndian.PutUint64(iBufW[:], j)
			ge.Throw(job.SetData([]byte("j"), iBufW[:]))

			time.Sleep(time.Millisecond * 100)
		}

		if j < 1000 {
			return errors.New("i couldn't reach to 10; i = " + strconv.Itoa(int(j)))
		}

		return nil
	}

	ge.Throw(queue.Add("w_on_i", woni))
	ge.Throw(queue.Add("w_on_j", wonj))

	go func() {
		err := queue.Process()
		fmt.Println("queue error before cancel was : ", err)
	}()

	terminate(queue, 2, 1)
	time.Sleep(time.Second)

	// continue processing the same jobs if their not finished on other startup
	queue2 := NewJobQueue(context.Background(), "queue-1", db)

	ge.Throw(queue2.Add("w_on_i", woni))
	ge.Throw(queue2.Add("w_on_j", wonj))

	go func() {
		terminate(queue2, 5, 10)
	}()

	ge.Throw(queue2.Process())
}
