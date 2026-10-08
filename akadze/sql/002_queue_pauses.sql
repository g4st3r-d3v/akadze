CREATE TABLE akadze.queue_pauses (
    queue text PRIMARY KEY,
    paused_at timestamptz NOT NULL DEFAULT now()
);
