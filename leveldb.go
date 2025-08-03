package jobqueue

import (
	"context"
	"github.com/syndtr/goleveldb/leveldb"
)

var _ Database = &JobLevelDBWrapper{}

type JobLevelDBWrapper struct {
	db *leveldb.DB
}

func NewJobLevelDBWrapper(db *leveldb.DB) *JobLevelDBWrapper {
	return &JobLevelDBWrapper{
		db: db,
	}
}

func (jdb *JobLevelDBWrapper) Set(ctx context.Context, workerID string, jobID string, key []byte, value []byte) error {
	k := append([]byte("____"+jobID+"_"), key...)
	return jdb.db.Put(k, value, nil)
}

func (jdb *JobLevelDBWrapper) Get(ctx context.Context, workerID string, jobID string, key []byte) ([]byte, error) {
	k := append([]byte("____"+jobID+"_"), key...)
	return jdb.db.Get(k, nil)
}

func (jdb *JobLevelDBWrapper) Has(ctx context.Context, workerID string, jobID string, key []byte) (bool, error) {
	k := append([]byte("____"+jobID+"_"), key...)
	return jdb.db.Has(k, nil)
}
