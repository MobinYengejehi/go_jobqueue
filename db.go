package jobqueue

import "context"

type Database interface {
	Set(ctx context.Context, workerID string, jobID string, key []byte, value []byte) error
	Get(ctx context.Context, workerID string, jobID string, key []byte) ([]byte, error)
	Has(ctx context.Context, workerID string, jobID string, key []byte) (bool, error)
}
