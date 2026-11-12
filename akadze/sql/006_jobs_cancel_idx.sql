-- The cancel step closes queued jobs whose cancel was requested or that expired.
-- Without this index it reads every queued job of the queue on each claim pass.
-- Partial: only rows that can be cancelled, so it stays small and cheap to write.
CREATE INDEX IF NOT EXISTS jobs_cancel_idx
    ON akadze.jobs (queue)
    WHERE state = 'queued' AND (cancel_requested_at IS NOT NULL OR expires_at IS NOT NULL);
