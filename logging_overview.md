# Python logging: deep walk-through (init_source_preparation example)

## Big picture
Logging is a structured replacement for `print()`. You emit events with metadata (time, level, logger name), and the logging system decides:
- what to keep (level filtering)
- how to format it (formatters)
- where to send it (handlers)

## Core building blocks
- **Logger**: the object you call (`logger.info(...)`); it creates LogRecords.
- **LogRecord**: the event with timestamp, level, logger name, message, etc.
- **Handler**: output destination (stdout, file, etc.).
- **Formatter**: converts LogRecord into text.
- **Filter**: optional rules to allow/deny records.

## Logger hierarchy
- Logger names form a tree: `foo`, `foo.bar`, `foo.bar.baz`.
- If a logger has no handlers, it **propagates** to its parent.
- The **root logger** is the final parent that usually holds handlers.

---

## How our logging is wired (init_source_preparation)

### 1) Configuration (one time)
`docker/init_source_preparation/model/logging_utils.py`:

```python
def setup_logging(level="INFO"):
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )
```

What this does:
- `level="INFO"` means INFO and higher are emitted.
- `format` includes timestamp, level, logger name, and message.
- `StreamHandler(sys.stdout)` sends logs to stdout (Airflow captures it).
- `force=True` removes any previous handlers so formatting is consistent.

---

### 2) Entry point calls setup_logging
`docker/init_source_preparation/model/__main__.py`:

```python
setup_logging()
logger = logging.getLogger(APP_NAME)
logger.info("Starting init source preparation ...")
...
logger.info("Init source preparation completed")
```

Effect:
- After `setup_logging()`, the root logger is configured.
- The `logger` has name `init-source-preparation` (APP_NAME).
- It propagates to root and is written to stdout.

---

### 3) Other modules just use the logger
Example: `docker/init_source_preparation/model/db.py`

```python
logger = logging.getLogger(APP_NAME)

logger.info("Connecting to Postgres via SQLAlchemy ...")
...
logger.info("Bulk insert starting: schema.table (rows=...)")
```

Why this works:
- Each module reuses the same logger name.
- No extra setup is needed.
- All logs flow to the root handler from `setup_logging()`.

---

## End-to-end flow for a log call

Example line:

```python
logger.info("Creating %s", table_name)
```

Steps:
1) Logger creates a LogRecord with:
   - name = `init-source-preparation`
   - level = INFO
   - message + args
   - timestamp, module, etc.
2) Logger propagates to the root logger.
3) Root logger checks level (INFO >= INFO).
4) Root handler formats the record.
5) Handler writes to stdout.

---

## Example output in Airflow logs

```
2025-12-26 10:15:12 | INFO | init-source-preparation | Starting init source preparation (schema=source, clients=20000, delivery_resources=700)
2025-12-26 10:15:13 | INFO | init-source-preparation | Creating clients
2025-12-26 10:15:15 | INFO | init-source-preparation | Bulk insert completed: 20000 records into source.clients
2025-12-26 10:15:20 | INFO | init-source-preparation | Init source preparation completed
```

---

## Why it is better than print()
- Levels allow changing verbosity without code edits.
- Consistent formatting makes logs easy to scan.
- Logger name is a stable correlation id.
- Centralized configuration; modules just log.

---

## Optional: control verbosity via env var
You can later do:

```python
setup_logging(level=os.getenv("LOG_LEVEL", "INFO"))
```

Then set `LOG_LEVEL=DEBUG` in the environment.
