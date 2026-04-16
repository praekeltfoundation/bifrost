# bifrost

Bifrost synchronises patient data between the SyNCH CCMDD APIs and us.

## Requirements

- Python 3.10 or newer
- `uv`

## Installation

1. Apply migrations:

```bash
uv run ./manage.py migrate
```

2. Create an admin user if you want to use the Django admin:

```bash
uv run ./manage.py createsuperuser
```

## Running Locally

Start the Django development server:

```bash
uv run ./manage.py runserver
```

To run Celery against RabbitMQ locally, point the `CELERY_BROKER_URL` environment variable at your broker if you are not using the default guest account on `localhost`.

Start a worker:

```bash
uv run celery -A bifrost worker --loglevel=info
```

Start Celery Beat:

```bash
uv run celery -A bifrost beat --loglevel=info
```

Useful local URLs:

- App health endpoint: `http://127.0.0.1:8000/health`
- Django admin: `http://127.0.0.1:8000/admin/`


## Full Verification

After every code change, run the full local verification suite:

```bash
uv run ruff format .
uv run ruff check --fix .
uv run mypy .
uv run ./manage.py test
```

## Documentation

- [Lock app](docs/lock.md)
