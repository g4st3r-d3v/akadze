CREATE INDEX jobs_meta_idx
    ON akadze.jobs USING gin (meta jsonb_path_ops);
