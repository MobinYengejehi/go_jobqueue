# JobQueue

`JobQueue` is a lightweight worker that processes jobs one at a time. Each job runs inside a pausable context and can store progress in an optional database.

## Creating a queue

```go
queue := jobqueue.NewJobQueue(context.Background(), "worker-1", nil)
```

The third argument is a `Database` implementation. Pass `nil` if you do not need to persist job data.

## Adding and processing jobs

```go
_ = queue.Add("print", func(job jobqueue.ImmutableJobQueue) error {
    for i := 0; job.Running() && i < 3; i++ {
        fmt.Println("number", i)
        time.Sleep(time.Second)
    }
    return nil
})

if err := queue.Process(); err != nil {
    log.Println("queue failed:", err)
}
```

`Running()` returns `false` when the queue is cancelled or the context is done.

## Pausing and cancelling

```go
go func() {
    time.Sleep(2 * time.Second)
    queue.Pause()
    fmt.Println("paused")
    time.Sleep(time.Second)
    queue.Unpause()
    fmt.Println("unpaused")
    time.Sleep(time.Second)
    queue.Cancel()
}()
```

`Pause` and `Unpause` allow cooperative pausing. `Cancel` stops the queue and all jobs immediately.

## Storing data

When a `Database` is supplied, each job can persist arbitrary `[]byte` data:

```go
key := []byte("progress")
if data, _ := job.GetData(key); data != nil {
    // resume work using saved data
}
_ = job.SetData(key, []byte("42"))
```

This makes it possible to resume unfinished jobs across restarts.
