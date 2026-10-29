# Examples

Runnable projects live in [`examples/`](https://github.com/g4st3r-d3v/akadze/tree/main/examples) in the repository. CI runs each one against the package from the same commit, so they keep working.

## FastAPI

[`examples/fastapi`](https://github.com/g4st3r-d3v/akadze/tree/main/examples/fastapi): an order service.

- `POST /orders` writes the order and enqueues `orders.confirm` in one transaction: a failed request leaves neither.
- The worker confirms the order and enqueues `orders.receipt` on queue `mail` in the transaction that marks the job `succeeded` ([`complete_tx`](guides/retries.md#commit-your-writes-with-the-outcome)).
- `setup_db.py` is the deploy step: `migrate`, then the app's own tables.
- `test_example.py` drives the API with `httpx` and runs the jobs with `drain`.
- `compose.yml` starts Postgres, the setup step, the API and a worker on both queues.

```bash
cd examples/fastapi
docker compose up --build
curl -X POST localhost:8000/orders -H 'content-type: application/json' -d '{"email": "ada@example.com"}'
curl localhost:8000/orders/1
```
