# FastAPI + akadze

An order service. `POST /orders` writes the order and enqueues `orders.confirm` in one transaction. The worker confirms the order and enqueues `orders.receipt` on queue `mail` in the transaction that marks the job `succeeded` (`complete_tx`).

| File | What it shows |
|---|---|
| `main.py` | the `Akadze` app, `include("tasks")`, enqueue inside the request's transaction |
| `tasks.py` | `register(app)`, `complete_tx`, a follow-up job on another queue |
| `orders.py` | the application's own table, outside schema `akadze` |
| `setup_db.py` | the deploy step: `migrate`, then the app's tables |
| `test_example.py` | `httpx` against the app, `assert_enqueued`, `drain`, `list_jobs` |
| `compose.yml` | Postgres, setup, API and worker, one image |

Run it:

```bash
docker compose up --build
curl -X POST localhost:8000/orders -H 'content-type: application/json' -d '{"email": "ada@example.com"}'
curl localhost:8000/orders/1          # "status": "confirmed" once the worker ran
docker compose exec worker akadze jobs list
```

Test it against a database whose name ends in `_test` (the tests drop schema `akadze` and the example table there):

```bash
pip install -r requirements.txt -r requirements-test.txt
AKADZE_DATABASE_URL=postgresql://akadze:akadze@localhost:5432/example_test pytest
```

The image installs akadze from PyPI (`~=0.6.0`); CI runs the tests against the package from the same commit.
