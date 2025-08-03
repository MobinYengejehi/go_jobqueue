package jobqueue

import (
	"errors"
	"sync"
)

type hQueue struct {
	handlers map[string]JobHandler
	mu       sync.Mutex
}

func newHandlerQueue() *hQueue {
	return &hQueue{
		handlers: make(map[string]JobHandler),
	}
}

func (que *hQueue) exists(id string) bool {
	_, exists := que.handlers[id]
	return exists
}

func (que *hQueue) add(id string, handler JobHandler) error {
	que.mu.Lock()
	defer que.mu.Unlock()
	if que.exists(id) {
		return errors.New("job already exists")
	}
	que.handlers[id] = handler
	return nil
}

func (que *hQueue) remove(id string) error {
	que.mu.Lock()
	defer que.mu.Unlock()
	if !que.exists(id) {
		return errors.New("job doesn't exist")
	}
	delete(que.handlers, id)
	return nil
}

func (que *hQueue) done(id string) error {
	return que.remove(id)
}

func (que *hQueue) next() (string, JobHandler) {
	que.mu.Lock()
	defer que.mu.Unlock()
	for id, handler := range que.handlers {
		return id, handler
	}
	return "", nil
}

func (que *hQueue) len() int {
	que.mu.Lock()
	defer que.mu.Unlock()
	return len(que.handlers)
}
