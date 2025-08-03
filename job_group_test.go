package jobqueue

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/itsabgr/ge"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"runtime"
	"strconv"
	"testing"
	"time"
)

func TestJobGroup(t *testing.T) {
	jGroup, _ := NewJobGroup(context.Background(), nil)
	defer jGroup.Cancel()
	jGroup.SetLimit(3)

	ge.Throw(jGroup.Go("w_on_i", func(job ImmutableJobQueue) error {
		fmt.Println("staring job : ", job.WorkerID(), " | ", job.JobID())

		i := 0
		for ; job.Running() && i < 35; i++ {
			fmt.Println("processing i : ", i)
			time.Sleep(time.Millisecond * 500)
		}

		if i < 30 {
			return errors.New("i couldn't reach to 30")
		}

		return nil
	}))
	ge.Throw(jGroup.Go("w_on_j", func(job ImmutableJobQueue) error {
		fmt.Println("starting job : ", job.WorkerID(), " | ", job.JobID())

		j := 0
		for ; job.Running() && j < 55; j++ {
			fmt.Println("processing j : ", j)
			time.Sleep(time.Millisecond * 100)
		}

		if j < 50 {
			return errors.New("j couldn't reach to 50")
		}

		return nil
	}))

	for x := range 20 {
		ge.Throw(jGroup.Go("w_te_"+strconv.Itoa(x), func(job ImmutableJobQueue) error {
			if !job.Running() {
				return nil
			}
			fmt.Println("writing x in : ", job.WorkerID(), " | ", job.JobID(), " | ", x)
			time.Sleep(time.Second * 2)
			return nil
		}))
	}

	go func() {
		pauseTest(jGroup, 5, 0)
	}()

	ge.Throw(jGroup.Wait())

	fmt.Println("done queue: ", runtime.NumCPU(), " | ", runtime.NumGoroutine())
}

func TestJobGroupWithDatabase(t *testing.T) {
	ldb := ge.Must(leveldb.Open(storage.NewMemStorage(), nil))
	defer ldb.Close()
	db := NewJobLevelDBWrapper(ldb)

	jGroup, _ := NewJobGroup(context.Background(), db)
	defer jGroup.Cancel()

	ge.Throw(jGroup.Go("job-1", job1))
	ge.Throw(jGroup.Go("job-2", job2))

	go func() {
		err := jGroup.Wait()
		if err != nil {
			fmt.Println("group err is : ", err)
		}
	}()

	pauseTest(jGroup, 5, 3)
	fmt.Println("job group canceled. trying to start it again...")
	time.Sleep(time.Second * 2)

	// new job group which will continue processing from where it was saved
	group2, _ := NewJobGroup(context.Background(), db)
	defer group2.Cancel()

	ge.Throw(group2.Go("job-1", job1))
	ge.Throw(group2.Go("job-2", job2))

	ge.Throw(group2.Wait())
}

func job1(job ImmutableJobQueue) error {
	fmt.Println("starting job : ", job.WorkerID(), " | ", job.JobID())

	x := loadJobData(job, "my-x")
	fmt.Println("starting new job with x : ", x)
	time.Sleep(time.Second * 3)

	for ; job.Running() && x < 35; x++ {
		fmt.Println("processing x : ", x)
		time.Sleep(time.Millisecond * 500)
		saveJobData(job, "my-x", x)
	}

	if x < 30 {
		return errors.New("x couldn't reach to 30: x = " + strconv.Itoa(int(x)))
	}

	return nil
}

func job2(job ImmutableJobQueue) error {
	fmt.Println("starting job : ", job.WorkerID(), " | ", job.JobID())

	y := loadJobData(job, "my-y")
	fmt.Println("starting new job with y : ", y)
	time.Sleep(time.Second * 3)

	for ; job.Running() && y < 55; y++ {
		fmt.Println("processing y : ", y)
		time.Sleep(time.Millisecond * 100)
		saveJobData(job, "my-y", y)
	}

	if y < 50 {
		return errors.New("y couldn't reach to 50; y = " + strconv.Itoa(int(y)))
	}

	return nil
}

func loadJobData(job ImmutableJobQueue, key string) uint64 {
	v := uint64(0)
	k := []byte(key)
	if ge.Must(job.HasData(k)) {
		buf := ge.Must(job.GetData(k))
		v = binary.LittleEndian.Uint64(buf)
	}
	return v
}

func saveJobData(job ImmutableJobQueue, key string, value uint64) {
	k := []byte(key)
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], value)
	ge.Throw(job.SetData(k, buf[:]))
}

func pauseTest(jGroup *JobGroup, ps time.Duration, cs time.Duration) {
	time.Sleep(time.Second * ps)
	jGroup.SetPaused(true)
	fmt.Println("pausing the group")
	time.Sleep(time.Second * ps)
	jGroup.SetPaused(false)
	fmt.Println("unpausing the group")
	if cs > 0 {
		time.Sleep(time.Second * cs)
		jGroup.Cancel()
		fmt.Println("job canceled")
	}
}
