CREATE INDEX IF NOT EXISTS jobs_claim_key_idx
    ON akadze.jobs (queue, limit_key, priority DESC, run_at, id)
    WHERE state = 'queued';
