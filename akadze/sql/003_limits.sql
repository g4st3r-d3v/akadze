ALTER TABLE akadze.jobs
    ADD COLUMN limit_key text;

CREATE TABLE akadze.limits (
    key text PRIMARY KEY,
    max_running int NOT NULL,
    CONSTRAINT limits_max_running_check CHECK (max_running >= 1)
);

CREATE INDEX jobs_running_limit_key_idx
    ON akadze.jobs (limit_key)
    WHERE state = 'running' AND limit_key IS NOT NULL;
