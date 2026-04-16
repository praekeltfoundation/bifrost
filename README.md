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

## Docker

The Docker image uses `bifrost.settings.production`, which requires the `SECRET_KEY`, `ALLOWED_HOSTS`, `DATABASE_URL`, and `CSRF_TRUSTED_ORIGINS` environment variables to be set at runtime.

GitHub Actions publishes the image to `ghcr.io/<owner>/<repo>` on every branch push using a branch-prefixed SHA tag, and on pushes of tags matching `v*` using the semantic version tag.

Build the image:

```bash
docker build -t bifrost .
```

To run the app against PostgreSQL in Docker, start a database container on a shared network and point `DATABASE_URL` at it:

```bash
docker network create bifrost

docker run -d --rm \
  --name bifrost-db \
  --network bifrost \
  -e POSTGRES_DB=bifrost \
  -e POSTGRES_USER=bifrost \
  -e POSTGRES_PASSWORD=bifrost \
  postgres:16

docker run --rm -p 8000:8000 \
  --network bifrost \
  -e SECRET_KEY=change-me \
  -e ALLOWED_HOSTS='*' \
  -e DATABASE_URL='postgresql://bifrost:bifrost@bifrost-db:5432/bifrost' \
  bifrost
```


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
