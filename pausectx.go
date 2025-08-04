package jobqueue

import (
	"context"
	"sync"
	"time"
)

var _ PausableContext = &pausableContextT{}

type ImmutablePausableContext interface {
	context.Context
	Paused() bool
	WaitIfPaused()
}

type PausableContext interface {
	ImmutablePausableContext
	Pause()
	Unpause()
}

type pausableContextT struct {
	ctx context.Context

	mu     sync.Mutex
	cond   *sync.Cond
	paused bool
}

func NewPausableContext(parent context.Context) (PausableContext, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	pCtx := &pausableContextT{
		ctx:    ctx,
		paused: false,
	}
	pCtx.cond = sync.NewCond(&pCtx.mu)
	return pCtx, func() {
		pCtx.Unpause()
		cancel()
	}
}

func (pCtx *pausableContextT) Pause() {
	pCtx.mu.Lock()
	pCtx.paused = true
	pCtx.mu.Unlock()
}

func (pCtx *pausableContextT) Unpause() {
	pCtx.mu.Lock()
	pCtx.paused = false
	pCtx.cond.Broadcast()
	pCtx.mu.Unlock()
}

func (pCtx *pausableContextT) Paused() bool {
	pCtx.mu.Lock()
	paused := pCtx.paused
	pCtx.mu.Unlock()
	return paused
}

func (pCtx *pausableContextT) WaitIfPaused() {
	pCtx.mu.Lock()
	for pCtx.paused {
		pCtx.cond.Wait()
	}
	pCtx.mu.Unlock()
}

func (pCtx *pausableContextT) Deadline() (deadline time.Time, ok bool) {
	return pCtx.ctx.Deadline()
}

func (pCtx *pausableContextT) Done() <-chan struct{} {
	return pCtx.ctx.Done()
}

func (pCtx *pausableContextT) Err() error {
	return pCtx.ctx.Err()
}

func (pCtx *pausableContextT) Value(key any) any {
	return pCtx.ctx.Value(key)
}
