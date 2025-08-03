package jobqueue

import (
	"context"
	"errors"
	"github.com/itsabgr/ge"
	"strconv"
	"sync"
	"sync/atomic"
)

type JobGroup struct {
	ctx     context.Context
	cancel  context.CancelFunc
	db      Database
	jobs    map[string]JobQueue
	jobIds  []string
	jobIdx  int
	jmu     sync.Mutex
	wg      sync.WaitGroup
	err     error
	errMu   sync.Mutex
	limit   atomic.Int64
	counter atomic.Uint64
}

func NewJobGroup(ctx context.Context, db Database) (*JobGroup, context.Context) {
	pCtx, cancel := context.WithCancel(ctx)
	group := &JobGroup{
		ctx:    pCtx,
		cancel: cancel,
		db:     db,
		jobs:   make(map[string]JobQueue),
		jobIds: make([]string, 0),
		jobIdx: 0,
		err:    nil,
	}
	group.limit.Store(0)
	return group, pCtx
}

func (jGroup *JobGroup) Go(id string, handler JobHandler) error {
	if jGroup.mustAppend() {
		return jGroup.append(id, handler)
	}
	return jGroup.spawnJobQueue(id, handler)
}

func (jGroup *JobGroup) append(id string, handler JobHandler) error {
	jGroup.jmu.Lock()
	defer jGroup.jmu.Unlock()

	if len(jGroup.jobIds) < 1 {
		return errors.New("couldn't find any workers")
	}

	workerId := jGroup.jobIds[int(jGroup.counter.Load())%len(jGroup.jobIds)]
	queue := jGroup.jobs[workerId]

	err := queue.Add(id, handler)

	jGroup.counter.Add(1)

	return err
}

func (jGroup *JobGroup) mustAppend() bool {
	jGroup.jmu.Lock()
	defer jGroup.jmu.Unlock()
	limit := jGroup.limit.Load()
	if limit > 0 {
		return len(jGroup.jobs) >= int(limit)
	}
	return false
}

func (jGroup *JobGroup) Wait() error {
	jGroup.wg.Wait()
	return jGroup.err
}

func (jGroup *JobGroup) SetPaused(paused bool) {
	jGroup.jmu.Lock()
	defer jGroup.jmu.Unlock()
	for _, job := range jGroup.jobs {
		if paused {
			job.Pause()
		} else {
			job.Unpause()
		}
	}
}

func (jGroup *JobGroup) Cancel() {
	jGroup.jmu.Lock()
	defer jGroup.jmu.Unlock()
	// calling job.Cancel is necessary for pause. if a PausableContext be paused it will only unpause when its own cancel function get called
	for _, job := range jGroup.jobs {
		job.Cancel()
	}
	jGroup.cancel()
}

func (jGroup *JobGroup) SetLimit(limit int64) {
	jGroup.limit.Store(limit)
}

func (jGroup *JobGroup) spawnJobQueue(id string, handler JobHandler) error {
	jGroup.jmu.Lock()
	defer jGroup.jmu.Unlock()

	workerID := "worker-" + strconv.Itoa(jGroup.jobIdx)
	jGroup.jobIdx++
	exists := false
	for _, exists = jGroup.jobs[workerID]; exists; _, exists = jGroup.jobs[workerID] {
		workerID = "worker-" + strconv.Itoa(jGroup.jobIdx)
		jGroup.jobIdx++
	}

	job := NewJobQueue(jGroup.ctx, workerID, jGroup.db)
	jGroup.jobs[workerID] = job

	if err := job.Add(id, handler); err != nil {
		delete(jGroup.jobs, workerID)
		return err
	}

	jGroup.collectJobIds()

	jGroup.wg.Add(1)
	jGroup.counter.Add(1)

	go func() {
		jGroup.collectErr(job.Process())
		jGroup.collectErr(jGroup.doneJobQueue(workerID))
	}()

	return nil
}

func (jGroup *JobGroup) doneJobQueue(workerID string) error {
	jGroup.jmu.Lock()
	defer jGroup.jmu.Unlock()
	defer jGroup.wg.Done()

	if _, exists := jGroup.jobs[workerID]; !exists {
		return errors.New("job queue doesn't exist")
	}

	jGroup.jobs[workerID].Cancel()
	delete(jGroup.jobs, workerID)

	jGroup.collectJobIds()

	return nil
}

func (jGroup *JobGroup) collectJobIds() {
	jGroup.jobIds = make([]string, 0, len(jGroup.jobs))
	for id := range jGroup.jobs {
		jGroup.jobIds = append(jGroup.jobIds, id)
	}
}

func (jGroup *JobGroup) collectErr(err error) {
	jGroup.errMu.Lock()
	defer jGroup.errMu.Unlock()
	if err != nil {
		jGroup.err = ge.Join(jGroup.err, err)
	}
}
