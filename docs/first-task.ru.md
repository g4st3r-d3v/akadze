# Первая задача

Здесь путь от пустой базы до выполненного задания. Нужны Python 3.13 и база PostgreSQL, в которой можно создать схему.

## 1. Установка и миграции

```bash
pip install akadze
export AKADZE_DATABASE_URL=postgresql://akadze:akadze@localhost:5432/akadze
akadze migrate
```

`akadze migrate` создаёт схему `akadze` и её таблицы. Они не входят в миграции вашего приложения: не копируйте их в Alembic. При деплое запускайте `akadze migrate` (или `await akadze.migrate(database_url)`) на той же базе.

## 2. Задача и постановка в очередь

Сохраните как `demo.py`:

```python
import asyncio

from akadze import Akadze

app = Akadze(database_url="postgresql://akadze:akadze@localhost:5432/akadze")


@app.task("hello")
async def hello(name: str) -> str:
    return f"hello {name}"


async def main() -> None:
    async with app.engine.begin() as connection:
        await hello.using(session=connection).enqueue(name="ada")
    await app.aclose()


if __name__ == "__main__":
    asyncio.run(main())
```

`enqueue` выполняется в транзакции вызывающего кода, поэтому откат удаляет задание. Аргументы передаются по имени и проверяются по сигнатуре функции.

```bash
python demo.py
```

## 3. Запуск воркера

Запустите из каталога, где лежит `demo.py`:

```bash
akadze worker demo:app
```

Воркер загружает `demo:app`, забирает задание, выполняет `hello` и переводит строку в `succeeded`. Проверка:

```bash
akadze jobs list --state succeeded
```

Остановите воркер через Ctrl-C. Он перестаёт брать новые задания и до 10 секунд ждёт выполняющиеся. Всё, что не успело завершиться, возвращается в очередь, попытка при этом не тратится.

## Дальше

- Разнести задачи по модулям и передать зависимости: [Задачи и приложение](guides/tasks.md).
- Задержки, приоритеты, дедупликация, массовая постановка: [Постановка в очередь](guides/enqueue.md).
- Что происходит, когда задача падает: [Ретраи, отмена и завершение](guides/retries.md).
