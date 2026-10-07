CREATE SCHEMA IF NOT EXISTS akadze;

CREATE TABLE akadze.jobs (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    task text NOT NULL,
    queue text NOT NULL DEFAULT 'default',
    priority smallint NOT NULL DEFAULT 0,
    state text NOT NULL DEFAULT 'queued',
    args jsonb NOT NULL DEFAULT '{}',
    meta jsonb NOT NULL DEFAULT '{}',
    run_count integer NOT NULL DEFAULT 0,
    attempt integer NOT NULL DEFAULT 0,
    max_attempts integer NOT NULL DEFAULT 3,
    snoozes integer NOT NULL DEFAULT 0,
    run_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz,
    unique_key text,
    worker_id uuid,
    cancel_requested_at timestamptz,
    errors jsonb NOT NULL DEFAULT '[]',
    result jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,
    finished_at timestamptz,
    CONSTRAINT jobs_state_check CHECK (
        state IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')
    ),
    CONSTRAINT jobs_attempt_check CHECK (attempt >= 0),
    CONSTRAINT jobs_run_count_check CHECK (run_count >= 0),
    CONSTRAINT jobs_max_attempts_check CHECK (max_attempts >= 1),
    CONSTRAINT jobs_snoozes_check CHECK (snoozes >= 0)
);

COMMENT ON COLUMN akadze.jobs.priority IS 'Higher values are claimed first.';
COMMENT ON COLUMN akadze.jobs.run_count IS 'Fencing token. Incremented on every claim.';
COMMENT ON COLUMN akadze.jobs.attempt IS
    'Failed runs. Snooze and a clean shutdown release do not increment it.';
COMMENT ON COLUMN akadze.jobs.result IS 'Optional small JSON. Large results stay outside the row.';

CREATE INDEX jobs_claim_idx
    ON akadze.jobs (queue, priority DESC, run_at, id)
    WHERE state = 'queued';

CREATE INDEX jobs_running_worker_idx
    ON akadze.jobs (worker_id)
    WHERE state = 'running';

CREATE UNIQUE INDEX jobs_unique_key_active_idx
    ON akadze.jobs (unique_key)
    WHERE unique_key IS NOT NULL AND state IN ('queued', 'running');

CREATE INDEX jobs_finished_at_idx
    ON akadze.jobs (finished_at)
    WHERE state IN ('succeeded', 'failed', 'cancelled');

ALTER TABLE akadze.jobs SET (
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_vacuum_threshold = 1000,
    autovacuum_analyze_scale_factor = 0.01,
    autovacuum_analyze_threshold = 1000
);

CREATE TABLE akadze.workers (
    id uuid PRIMARY KEY,
    hostname text NOT NULL,
    pid integer NOT NULL,
    queues text[] NOT NULL,
    started_at timestamptz NOT NULL DEFAULT now(),
    heartbeat_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE akadze.periodic_runs (
    name text NOT NULL,
    fire_at timestamptz NOT NULL,
    job_id bigint,
    PRIMARY KEY (name, fire_at)
);
