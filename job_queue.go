package jobqueue

import (
	"context"
	"fmt"
	"github.com/itsabgr/ge"
	"sync"
)

var _ JobQueue = &JobQueueT{}

type JobHandler func(job ImmutableJobQueue) error

type ImmutableJobQueue interface {
	Paused() bool
	WaitIfPaused()
	Running() bool
	WorkerID() string
	JobID() string
	Context() ImmutablePausableContext
	SetData(key []byte, value []byte) error
	GetData(key []byte) ([]byte, error)
	HasData(key []byte) (bool, error)
}

type JobQueue interface {
	ImmutableJobQueue
	Process() error
	Add(id string, handler JobHandler) error
	Remove(id string) error
	Cancel()
	Pause()
	Unpause()
	SetAutoPauseWaitEnabled(state bool)
}

type JobQueueT struct {
	workerID  string
	jobID     string
	queue     *hQueue
	pCtx      PausableContext
	pCancel   context.CancelFunc
	autoPause bool
	db        Database
	mu        sync.Mutex
}

func NewJobQueue(ctx context.Context, id string, db Database) JobQueue {
	pCtx, cancel := NewPausableContext(ctx)
	return &JobQueueT{
		workerID:  id,
		jobID:     "",
		queue:     newHandlerQueue(),
		pCtx:      pCtx,
		pCancel:   cancel,
		autoPause: true,
		db:        db,
	}
}

func (job *JobQueueT) Process() error {
	var mErr error = nil
	var id = ""
	var handler JobHandler = nil
	for id, handler = job.queue.next(); handler != nil; id, handler = job.queue.next() {
		job.jobID = id
		if err := ge.Try(func() {
			err := handler(job)
			if err != nil {
				mErr = ge.Join(mErr, err)
			}
		}); err != nil {
			mErr = ge.Join(mErr, fmt.Errorf("%v", err))
		}
		if err := job.queue.done(id); err != nil {
			mErr = ge.Join(mErr, err)
			return mErr
		}
	}
	job.jobID = ""
	return mErr
}

func (job *JobQueueT) Add(id string, handler JobHandler) error {
	return job.queue.add(id, handler)
}

func (job *JobQueueT) Remove(id string) error {
	return job.queue.remove(id)
}

func (job *JobQueueT) Cancel() {
	job.pCancel()
}

func (job *JobQueueT) Pause() {
	job.pCtx.Pause()
}

func (job *JobQueueT) Unpause() {
	job.pCtx.Unpause()
}

func (job *JobQueueT) Paused() bool {
	return job.pCtx.Paused()
}

func (job *JobQueueT) WaitIfPaused() {
	job.pCtx.WaitIfPaused()
}

func (job *JobQueueT) SetAutoPauseWaitEnabled(state bool) {
	job.mu.Lock()
	defer job.mu.Unlock()
	job.autoPause = state
}

func (job *JobQueueT) Running() bool {
	job.mu.Lock()
	autoPause := job.autoPause
	job.mu.Unlock()
	if autoPause {
		job.WaitIfPaused()
	}
	return job.pCtx.Err() == nil
}

func (job *JobQueueT) WorkerID() string {
	return job.workerID
}

func (job *JobQueueT) JobID() string {
	return job.jobID
}

func (job *JobQueueT) Context() ImmutablePausableContext {
	return job.pCtx
}

func (job *JobQueueT) SetData(key []byte, value []byte) error {
	if job.db == nil {
		return nil
	}
	job.mu.Lock()
	defer job.mu.Unlock()
	return job.db.Set(job.pCtx, job.workerID, job.jobID, key, value)
}

func (job *JobQueueT) GetData(key []byte) ([]byte, error) {
	if job.db == nil {
		return nil, nil
	}
	return job.db.Get(job.pCtx, job.workerID, job.jobID, key)
}

func (job *JobQueueT) HasData(key []byte) (bool, error) {
	if job.db == nil {
		return false, nil
	}
	return job.db.Has(job.pCtx, job.workerID, job.jobID, key)
}
