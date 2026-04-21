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

Sync uses these CCMDD settings:

- `CCMDD_BASE_URL`
- `CCMDD_USERNAME`
- `CCMDD_PASSWORD`

Sentry is optional. Bifrost only initializes Sentry when `SENTRY_DSN` is set. You can further configure it with `SENTRY_ENVIRONMENT`, `SENTRY_RELEASE`, `SENTRY_SEND_DEFAULT_PII`, `SENTRY_TRACES_SAMPLE_RATE`, `SENTRY_PROFILES_SAMPLE_RATE`, and `SENTRY_DEBUG`.

Start a worker:

```bash
uv run celery -A bifrost worker --loglevel=info
```

Start Celery Beat:

```bash
uv run celery -A bifrost beat --loglevel=info
```

Celery Beat schedules CCMDD synchronization once per day at `00:00` UTC. The scheduled task runs patient sync first and prescription sync second under a single top-level lock, so only one full CCMDD sync run can proceed at a time even if multiple workers are active.

Useful local URLs:

- App health endpoint: `http://127.0.0.1:8000/health`
- Django admin: `http://127.0.0.1:8000/admin/`

## Configuration
There is a base `bifrost.settings.base` settings module for local development, which is extended by `bifrost.settings.production` for running in production environments.

If `SENTRY_DSN` environment variable is set, sentry will be configured.

`bifrost.settings.production` requires the `SECRET_KEY`, `ALLOWED_HOSTS`, `DATABASE_URL`, and `CSRF_TRUSTED_ORIGINS` environment variables to be set at runtime.


## Docker
The docker image uses `bifrost.settings.production` by default, so the required environment variables must be set when running the container.

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
- [CCMDD client](docs/ccmdd-client.md)
- [Tasks](docs/tasks.md)
