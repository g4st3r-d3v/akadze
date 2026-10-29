# Постановка в очередь

Задание ставится на переданном вами соединении или сессии, внутри вашей транзакции:

```python
async with app.engine.begin() as connection:
    await hello.using(session=connection).enqueue(name="ada")
```

Аргументы задачи передаются только по имени. Параметры постановки никогда не попадают в аргументы задачи: они идут через `using(...)`.

## Параметры

| Параметр `using(...)` | Смысл |
|---|---|
| `session` | `AsyncConnection` или `AsyncSession`, на котором делается вставка (обязателен) |
| `delay` | выполнять не раньше, чем через этот `timedelta` |
| `run_at` | выполнять не раньше этого `datetime` с часовым поясом |
| `priority` | переопределяет приоритет задачи для этого задания; больший забирается раньше (smallint) |
| `max_attempts` | переопределяет бюджет попыток, от 1 до 2147483647 |
| `unique_key` | не больше одного активного (`queued` или `running`) задания на ключ |
| `limit_key` | ключ, к которому применяются [лимиты](limits.md) |
| `expires_at` | с часовым поясом; задание, не начатое к этому моменту, отменяется |
| `meta` | небольшой JSON-объект — метка задания для эксплуатации |

Неверный параметр бросает `EnqueueError`, и ничего не вставляется.

## Дедупликация

```python
from akadze import DuplicateJob

try:
    await sync_user.using(session=connection, unique_key=f"sync:{user_id}").enqueue(user_id=user_id)
except DuplicateJob:
    pass
```

Вторая постановка с тем же активным `unique_key` бросает `DuplicateJob`. Остальная часть вашей транзакции при этом не прерывается. Когда первое задание завершится, ключ снова свободен.

## Массовая постановка

```python
await notify.using(session=connection, priority=5).enqueue_many(
    {"user_id": user_id} for user_id in user_ids
)
```

`enqueue_many` вставляет по заданию на элемент с общими параметрами, по порядку, по тысяче строк на запрос: 10 000 заданий занимают около 0,1 с против 16 с циклом `enqueue`. Сначала проверяются все элементы; один неверный бросает `EnqueueError`, и ничего не вставляется. `unique_key` именует одно задание, поэтому `enqueue_many` его не принимает. Хуки `on_enqueue` вызываются для каждого задания.

## Метки через meta

```python
await charge.using(session=connection, meta={"tenant": "acme", "order_id": 42}).enqueue(order_id=42)
```

`meta` — JSON-объект со строковыми ключами, не больше 8 КиБ, с GIN-индексом. Это метка, а не место для аргументов или секретов: она видна в `JobSummary.meta`, в `Job`, который получают хуки, и в CLI. NaN, бесконечности и символ NUL отклоняются.

Фильтр по ней:

```python
from akadze import list_jobs

jobs = await list_jobs(app.engine, meta={"tenant": "acme"})
```

```bash
akadze jobs list --meta tenant=acme --meta order_id=42
```

Задание подходит, если его meta содержит все указанные пары. Значения в CLI разбираются как JSON, если это возможно, иначе берутся как текст.
