# Python API

Всё ниже импортируется из `akadze`, если в заголовке не сказано иное. Сигнатуры взяты из кода в `main`, описания — из docstring на английском.

## Приложение и задачи

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

## Воркер

::: akadze.Worker
    options:
      members: [serve, step]

## Исходы

::: akadze.Retry

::: akadze.Snooze

::: akadze.Fail

::: akadze.Cancel

::: akadze.BackoffPolicy

`BackoffPolicy` — это `Callable[[int], timedelta]`: принимает номер только что упавшей попытки и возвращает задержку до следующей. Должна быть обычной функцией или объектом, у которого `__call__` не async.

## Ошибки

::: akadze.AkadzeError

::: akadze.EnqueueError

::: akadze.DuplicateJob

## Задания

::: akadze.request_cancel

::: akadze.requeue

::: akadze.list_jobs

::: akadze.JobSummary

::: akadze.queue_snapshot

::: akadze.QueueSnapshot

::: akadze.StateCount

::: akadze.job.Job

## Очереди и лимиты

::: akadze.pause_queue

::: akadze.resume_queue

::: akadze.set_limit

::: akadze.clear_limit

::: akadze.list_limits

::: akadze.LimitSummary

## Воркеры и схема

::: akadze.list_workers

::: akadze.WorkerSummary

::: akadze.migrate

::: akadze.applied_versions

## Хуки

::: akadze.hooks.Hooks
    options:
      members: false

## Метрики (`akadze.metrics`)

::: akadze.metrics.render_metrics

## Тестирование (`akadze.testing`)

::: akadze.testing.drain

::: akadze.testing.assert_enqueued
