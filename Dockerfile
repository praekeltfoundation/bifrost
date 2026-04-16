FROM ghcr.io/praekeltfoundation/docker-django-bootstrap-nw:py3.10-buster

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    UV_PROJECT_ENVIRONMENT=/usr/local \
    UV_LINK_MODE=copy

WORKDIR /app

RUN pip install uv==0.11.7

COPY . /app

RUN uv sync --locked --no-dev --inexact

ENV DJANGO_SETTINGS_MODULE=bifrost.settings.production

RUN SECRET_KEY=build-secret-key uv run ./manage.py collectstatic --noinput

CMD [\
    "bifrost.wsgi:application",\
    "--timeout=120",\
    "--workers=2",\
    "--threads=4",\
    "--worker-class=gthread",\
    "--worker-tmp-dir=/dev/shm"\
]
