# go_jobqueue

A small library for building pausable job queues and groups in Go. Jobs run inside a cooperative, cancellable context and can persist progress through an optional database interface.

## Features

- Queue jobs and process them sequentially per worker
- Pause, resume, or cancel jobs and entire groups
- Distribute jobs across multiple workers with `JobGroup`
- Optional persistent storage via the `Database` interface

## Installation

```bash
go get github.com/MobinYengejehi/go_jobqueue
```

## Quick start

```go
queue := jobqueue.NewJobQueue(context.Background(), "worker-1", nil)
queue.Add("hello", func(job jobqueue.ImmutableJobQueue) error {
    fmt.Println("hello from", job.WorkerID())
    return nil
})
queue.Process()
```

For more complete examples and API details, see the documentation:

- [JobQueue](docs/job_queue.md)
- [JobGroup](docs/job_group.md)
- [Database](docs/database.md)
