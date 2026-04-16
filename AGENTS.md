# Bifrost
Bifrost is an app that periodically pulls data from the SyNCH CCMDD APIs, and stores it in our local databases.

## Development workflow

Use `uv` for all Python and Django commands in this repository.

- Use `uv run` for commands that execute Python tools or project entrypoints, including Django management commands, Ruff, Mypy, and tests.
- Do not prefix plain shell utilities such as `rg`, `sed`, `cat`, `git`, `ls`, or `find` with `uv run`.
- Do not hand-write Django migrations. Generate them with Django management commands, for example `uv run ./manage.py makemigrations`.

After every code change, run the full local verification suite:

- `uv run ruff format .`
- `uv run ruff check --fix .`
- `uv run mypy .`
- `uv run ./manage.py test`
