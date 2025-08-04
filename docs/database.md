# Database

`JobQueue` and `JobGroup` can persist job-specific data by using a `Database` implementation. The interface is minimal:

```go
type Database interface {
    Set(ctx context.Context, workerID, jobID string, key, value []byte) error
    Get(ctx context.Context, workerID, jobID string, key []byte) ([]byte, error)
    Has(ctx context.Context, workerID, jobID string, key []byte) (bool, error)
}
```

## LevelDB wrapper

This package provides `JobLevelDBWrapper`, which adapts a [LevelDB](https://github.com/syndtr/goleveldb) database:

```go
ldb := leveldb.OpenFile("job-data", nil)
db := jobqueue.NewJobLevelDBWrapper(ldb)
queue := jobqueue.NewJobQueue(context.Background(), "worker-1", db)
```

Within a job you can read and write data:

```go
key := []byte("counter")
if has, _ := job.HasData(key); has {
    data, _ := job.GetData(key)
    fmt.Println("saved value:", string(data))
}
_ = job.SetData(key, []byte("42"))
```

Implement your own `Database` to plug in a different storage backend.
