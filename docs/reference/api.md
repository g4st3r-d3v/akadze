# Python API

Everything below is importable from `akadze` unless the heading says otherwise. Signatures come from the code on `main`.

## App and tasks

::: akadze.Akadze

::: akadze.task.Task
    options:
      members: [using]

::: akadze.task.BoundTask
    options:
      members: [enqueue, enqueue_many]

::: akadze.current

::: akadze.context.Context
    options:
      members: [complete_tx]

## Worker

::: akadze.Worker
    options:
      members: [serve, step]

## Outcomes

::: akadze.Retry

::: akadze.Snooze

::: akadze.Fail

::: akadze.Cancel

::: akadze.BackoffPolicy

`BackoffPolicy` is `Callable[[int], timedelta]`: it takes the number of the attempt that just failed and returns the delay before the next one. It must be a plain (not async) function.

## Errors

::: akadze.AkadzeError

::: akadze.EnqueueError

::: akadze.DuplicateJob

## Jobs

::: akadze.request_cancel

::: akadze.requeue

::: akadze.list_jobs

::: akadze.JobSummary

::: akadze.queue_snapshot

::: akadze.QueueSnapshot

::: akadze.StateCount

::: akadze.job.Job

## Queues and limits

::: akadze.pause_queue

::: akadze.resume_queue

::: akadze.set_limit

::: akadze.clear_limit

::: akadze.list_limits

::: akadze.LimitSummary

## Workers and schema

::: akadze.list_workers

::: akadze.WorkerSummary

::: akadze.migrate

::: akadze.applied_versions

## Hooks

::: akadze.hooks.Hooks
    options:
      members: false

## Metrics (`akadze.metrics`)

::: akadze.metrics.render_metrics

## Testing (`akadze.testing`)

::: akadze.testing.drain

::: akadze.testing.assert_enqueued
