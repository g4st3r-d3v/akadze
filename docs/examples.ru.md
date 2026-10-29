# Примеры

Запускаемые проекты лежат в [`examples/`](https://github.com/g4st3r-d3v/akadze/tree/main/examples) в репозитории. CI прогоняет тесты каждого примера на пакете из того же коммита, так что они не ломаются незаметно; Docker-образы ставят соответствующий релиз с PyPI.

## FastAPI

[`examples/fastapi`](https://github.com/g4st3r-d3v/akadze/tree/main/examples/fastapi): сервис заказов.

- `POST /orders` записывает заказ и ставит `orders.confirm` в одной транзакции: упавший запрос не оставляет ни заказа, ни задания.
- Воркер подтверждает заказ и ставит `orders.receipt` в очередь `mail` в той же транзакции, что переводит задание в `succeeded` ([`complete_tx`](guides/retries.md#commit-your-writes-with-the-outcome)).
- `setup_db.py` — шаг деплоя: `migrate`, затем таблицы самого приложения.
- `test_example.py` вызывает API через `httpx` и выполняет задания через `drain`.
- `compose.yml` поднимает Postgres, шаг настройки, API и воркер на обе очереди.

```bash
cd examples/fastapi
docker compose up --build
curl -X POST localhost:8000/orders -H 'content-type: application/json' -d '{"email": "ada@example.com"}'
curl localhost:8000/orders/1
```
