-- The cancel step closes queued jobs whose cancel was requested or that expired.
-- Without these it reads every queued job of the queue on each claim pass.
-- Partial and split by condition: Postgres combines them (BitmapOr) and reads
-- only requested cancels and jobs whose expiry has passed, not every job with a TTL.
CREATE INDEX IF NOT EXISTS jobs_cancel_requested_idx
    ON akadze.jobs (queue)
    WHERE state = 'queued' AND cancel_requested_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS jobs_expires_idx
    ON akadze.jobs (queue, expires_at)
    WHERE state = 'queued' AND expires_at IS NOT NULL;
