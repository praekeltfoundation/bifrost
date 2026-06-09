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

The post-sync Turn import uses these settings:

- `TURN_BASE_URL`
- `TURN_TOKEN`

The OTP delivery API uses these settings:

- `TURN_OTP_TOKEN`
- `TURN_OTP_TEMPLATE_NAMESPACE`
- `TURN_OTP_TEMPLATE_NAME`
- `TURN_OTP_TEMPLATE_LANGUAGE`
- `OTP_DELIVERY_THROTTLE_RATE`

This repo also contains standalone Turn.io Lua apps under `turn_apps/`. They are separate deployables from Django with their own tests and packaging.

Current Turn app:

- `turn_apps/synch_delivery_failures`: suppresses SynCH reminders after permanent WhatsApp delivery failures (`131026`, `131050`) and records the first failure in `synch_delivery_failure_message_id`

Run Turn app commands from the app directory:

```bash
cd turn_apps/synch_delivery_failures
make test
make build
```

Sentry is optional. Bifrost only initializes Sentry when `SENTRY_DSN` is set. You can further configure it with `SENTRY_ENVIRONMENT`, `SENTRY_RELEASE`, `SENTRY_SEND_DEFAULT_PII`, `SENTRY_TRACES_SAMPLE_RATE`, `SENTRY_PROFILES_SAMPLE_RATE`, and `SENTRY_DEBUG`.

Start a worker:

```bash
uv run celery -A bifrost worker --loglevel=info
```

Start Celery Beat:

```bash
uv run celery -A bifrost beat --loglevel=info
```

Celery Beat schedules CCMDD synchronization every 5 minutes. The scheduled task syncs facilities, prescriptions, and patients, refreshes next-appointment contact fields in Turn for all patients, imports qualifying new patients into Turn, and then handles changed patient phone numbers under a single top-level lock, so only one full CCMDD sync run can proceed at a time even if multiple workers are active. Turn sync uses the most recent valid patient phone number across prescriptions, writes `synch_patient_id` from the raw CCMDD patient identifier, treats appointments as usable only when they are on or after today and resolve to a facility with a non-blank name, and falls back to the most recent prescription with a usable facility name whenever no usable upcoming appointment remains after that filtering. In that fallback case it keeps `synch_next_appointment_date` blank but still populates the facility contact fields. Changed phone numbers retire the previous Turn contact by disabling `synch_reminders`, then set `synch_new_user` on the new Turn contact after the appointment fields have been refreshed.

Useful local URLs:

- App health endpoint: `http://127.0.0.1:8000/health`
- API schema: `http://127.0.0.1:8000/api/schema/`
- Swagger docs: `http://127.0.0.1:8000/api/docs/`
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
uv run ty check
uv run ./manage.py test
```

## Documentation

- [Lock app](docs/lock.md)
- [CCMDD client](docs/ccmdd-client.md)
- [OTP delivery API](docs/otp-api.md)
- [Turn client](docs/turn-client.md)
- [Tasks](docs/tasks.md)
- [SynCH delivery failures Turn app](turn_apps/synch_delivery_failures/README.md)
