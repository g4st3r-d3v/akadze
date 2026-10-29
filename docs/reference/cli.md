# CLI

The `akadze` command ships with the package. `akadze worker` uses the engine of the app it loads; every other command that touches the database reads `AKADZE_DATABASE_URL` and exits 2 when it is not set.

## akadze

```text
usage: akadze [-h] [--version] {worker,beat,migrate,jobs,queues,limits,workers,metrics} ...

Postgres-backed task queue

positional arguments:
  {worker,beat,migrate,jobs,queues,limits,workers,metrics}
    worker              Claim and run registered tasks
    beat                Unused: periodic tasks run inside the worker
    migrate             Apply schema migrations
    jobs                Inspect and manage jobs
    queues              Pause and resume queues
    limits              Inspect and clear limit policies
    workers             Inspect registered workers
    metrics             Print Prometheus exposition text

options:
  -h, --help            show this help message and exit
  --version             show program's version number and exit
```

## akadze worker

```text
usage: akadze worker [-h] [--slots SLOTS] [--queue QUEUES] [--fair] app

positional arguments:
  app             Import path of an Akadze app, for example demo:app

options:
  -h, --help      show this help message and exit
  --slots SLOTS
  --queue QUEUES
  --fair          Within a priority, claim round-robin across limit_key groups
```

## akadze migrate

```text
usage: akadze migrate [-h]

options:
  -h, --help  show this help message and exit
```

## akadze jobs list

```text
usage: akadze jobs list [-h] [--queue QUEUE] [--state STATE] [--limit LIMIT] [--meta KEY=VALUE]

options:
  -h, --help        show this help message and exit
  --queue QUEUE
  --state STATE
  --limit LIMIT
  --meta KEY=VALUE  Keep jobs whose meta has KEY equal to VALUE (JSON if it parses, else text)
```

## akadze jobs requeue

```text
usage: akadze jobs requeue [-h] job_id

positional arguments:
  job_id

options:
  -h, --help  show this help message and exit
```

## akadze jobs cancel

```text
usage: akadze jobs cancel [-h] job_id

positional arguments:
  job_id

options:
  -h, --help  show this help message and exit
```

## akadze queues pause

```text
usage: akadze queues pause [-h] name

positional arguments:
  name

options:
  -h, --help  show this help message and exit
```

## akadze queues resume

```text
usage: akadze queues resume [-h] name

positional arguments:
  name

options:
  -h, --help  show this help message and exit
```

## akadze limits list

```text
usage: akadze limits list [-h]

options:
  -h, --help  show this help message and exit
```

## akadze limits clear

```text
usage: akadze limits clear [-h] key

positional arguments:
  key

options:
  -h, --help  show this help message and exit
```

## akadze workers list

```text
usage: akadze workers list [-h]

options:
  -h, --help  show this help message and exit
```

## akadze metrics

```text
usage: akadze metrics [-h] {render} ...

positional arguments:
  {render}
    render    Print Prometheus exposition text

options:
  -h, --help  show this help message and exit
```

