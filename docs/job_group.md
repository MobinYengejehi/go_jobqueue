# JobGroup

`JobGroup` manages multiple `JobQueue` workers. Jobs are dispatched to workers and processed concurrently. The group can limit the number of workers, pause all jobs, or cancel everything at once.

## Creating a group

```go
group, _ := jobqueue.NewJobGroup(context.Background(), nil)
group.SetLimit(2) // at most two workers at a time
```

As with `JobQueue`, pass a `Database` implementation to persist job data across restarts.

## Scheduling jobs

```go
group.Go("job-1", func(job jobqueue.ImmutableJobQueue) error {
    fmt.Println("running", job.JobID(), "on", job.WorkerID())
    return nil
})

group.Go("job-2", func(job jobqueue.ImmutableJobQueue) error {
    time.Sleep(time.Second)
    return nil
})
```

`Go` automatically creates workers until the limit is reached. Subsequent jobs are queued onto existing workers in a round-robin fashion.

## Waiting, pausing, and cancelling

```go
go func() {
    time.Sleep(2 * time.Second)
    group.SetPaused(true)
    fmt.Println("group paused")
    time.Sleep(time.Second)
    group.SetPaused(false)
    fmt.Println("group unpaused")
}()

if err := group.Wait(); err != nil {
    log.Println("group error:", err)
}
```

Calling `Cancel` stops all workers and their jobs. `Wait` blocks until all jobs are finished or cancelled.
