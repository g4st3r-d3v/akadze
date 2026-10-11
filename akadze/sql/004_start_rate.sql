ALTER TABLE akadze.limits
    ALTER COLUMN max_running DROP NOT NULL;

ALTER TABLE akadze.limits
    DROP CONSTRAINT limits_max_running_check;

ALTER TABLE akadze.limits
    ADD COLUMN max_starts int,
    ADD COLUMN "window" interval;

ALTER TABLE akadze.limits
    ADD CONSTRAINT limits_max_running_check
        CHECK (max_running IS NULL OR max_running >= 1),
    ADD CONSTRAINT limits_rate_check
        CHECK (
            (max_starts IS NULL AND "window" IS NULL)
            OR (
                max_starts IS NOT NULL
                AND "window" IS NOT NULL
                AND max_starts >= 1
                AND "window" > INTERVAL '0'
            )
        ),
    ADD CONSTRAINT limits_policy_check
        CHECK (max_running IS NOT NULL OR max_starts IS NOT NULL);

CREATE TABLE akadze.limit_starts (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    key text NOT NULL,
    started_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX limit_starts_key_started_at_idx
    ON akadze.limit_starts (key, started_at);
