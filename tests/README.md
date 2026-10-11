# akadze tests

Suite hits **real Postgres** via `AKADZE_DATABASE_URL` (database `akadze` or `*_test`). No in-memory DB; do not mock the engine for core queue behavior.

## Style

1. **AAA** — Arrange / Act / Assert; **one act** per test.
2. **Factories** — `tests/factories.py`: `JobFactory` / `insert_job` (incl. `finished_at` / `expires_at`), `LimitFactory` / `apply_limit`, `pause`, `insert_periodic_run`, `age_worker_heartbeat`, `bump_run_count`. Prefer these over raw SQL dumps in arrange.
3. **Parametrize** — states, filters, limit edges, claim fullness, task outcomes; keep cases small (no mega-tests).
4. **pytest-mock** — only non-DB boundaries (hooks, time helpers). Never replace Postgres.
5. **Commands vs queries** — commands return `None`; assert via follow-up queries / `tests.pg` helpers (`job_row`, `states_by_task`).

## Layout

| Path | Role |
|---|---|
| `conftest.py` | `database_url`, `app`, `fake` |
| `factories.py` | row builders + insert/apply helpers |
| `pg.py` | read helpers against the live DB |

Leave quiet files alone until they change. Schema-constraint checks in `test_migrate.py` may keep raw `INSERT` when the DB default/check itself is under test.
