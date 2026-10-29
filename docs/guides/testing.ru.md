# Тестирование

## Выполнить задания в процессе теста

`akadze.testing` выполняет ваши задачи без процесса воркера.

```python
from akadze.testing import assert_enqueued, drain

async def test_signup_sends_welcome(app, client):
    await client.post("/signup", json={"email": "ada@example.com"})

    await assert_enqueued(app, "mail.welcome", email="ada@example.com")
    await drain(app)
```

- `drain(app)` выполняет все задания, готовые сейчас, так же, как воркер. Задания, которые поставил запуск и которые готовы, тоже выполняются. Бросает `AkadzeError`, если задача готового задания не зарегистрирована в `app`. Очереди на паузе пропускаются. Claim по-прежнему применяет лимиты, поэтому задание, придержанное `max_starts`, заставляет `drain` делать шаги, пока не пройдёт окно; `drain(app, limit=...)` ограничивает число шагов и бросает `AkadzeError`, если после них остались готовые задания.
- `assert_enqueued(app, name, **arguments)` проверяет, что задание этой задачи в `queued` или `running` имеет такие аргументы (сравнение в JSON-форме).

## Тестовая база

Тестам нужен настоящий PostgreSQL. Собственные тесты akadze читают `AKADZE_DATABASE_URL` и удаляют схему `akadze` в этой базе, поэтому отказываются работать с базой, имя которой не `akadze` и не оканчивается на `_test`. Используйте то же правило у себя: отдельная база, миграция один раз на сессию через `await migrate(url)`.
