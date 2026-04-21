# Lock App

The `lock` app provides a small database-backed distributed lock for Celery tasks in Bifrost.

## What It Stores

The app defines a single model, `lock.models.Lock`, with these fields:

- `key`: the logical name of the lock, such as `"daily-sync"`. This is unique.
- `owner`: the process or caller that currently holds the lock.
- `ttl`: how long the lock should remain valid after acquisition or refresh.
- `expires_at`: when the lock becomes stale and may be taken over.
- `created_at` / `updated_at`: audit timestamps.

There is one database row per lock key.

## Core Behaviour

The lock is optimistic at the API level and enforced in the database:

1. A caller tries to create a lock row for a given `key`.
2. If the row does not exist, the lock is acquired immediately.
3. If the row already exists, the app locks that row with
   `SELECT ... FOR UPDATE`.
4. If the existing lock is still active and owned by someone else, acquisition
   fails.
5. If the existing lock has expired, or it is already owned by the same owner,
   the row is updated in place and the caller becomes the current owner.

This means lock ownership is coordinated through the database transaction rather than in-memory state, so it works across multiple Django processes.

## API

### `Lock.acquire(key, owner=None, ttl=None)`

Acquire a lock for `key`.

- If `owner` is omitted, a random UUID hex string is generated.
- If `ttl` is omitted on first acquisition, `Lock.DEFAULT_TTL` is used.
- `DEFAULT_TTL` is currently `1 hour`.
- If a lock row already exists and has expired, the same row is reused rather
  than deleted and recreated.

Returns the `Lock` instance on success.

Raises `LockAcquisitionError` when:

- the lock exists,
- `expires_at` is still in the future,
- and the current owner is different from the requested owner.

### `lock.refresh()`

Extend the current lock's expiry using its stored `ttl`.

The method reloads the row under `SELECT ... FOR UPDATE` and only succeeds if
the database row is still owned by the same `owner` as the instance calling
`refresh()`.

On success:

- if the lock was updated less than one minute ago, `refresh()` is a no-op
- otherwise, `expires_at` becomes `timezone.now() + lock.ttl`
- `updated_at` is updated

Raises `LockOwnershipError` when another owner has taken over the lock.

This includes the case where:

- the original lock expired, and
- another process reacquired the same `key`.

If the lock has expired but no other owner has taken it, `refresh()` still
works because the row owner is unchanged.

### `lock.release()`

Delete the lock row, but only if the caller still owns it.

The method reloads the row under `SELECT ... FOR UPDATE` before deleting it.

Raises `LockOwnershipError` when another owner has already taken over the lock.

## Ownership Model

The app uses ownership to prevent a stale process from extending or releasing a
lock it no longer controls.

Example:

1. `worker-1` acquires `daily-sync`.
2. The lock expires.
3. `worker-2` acquires `daily-sync`.
4. `worker-1` still has an old `Lock` instance in memory.
5. `worker-1.refresh()` and `worker-1.release()` must fail.

That is the main safety property of the app.

## Typical Usage

```python
from datetime import timedelta

from lock.models import Lock, LockAcquisitionError

owner = "sync-worker-1"

try:
    lock = Lock.acquire(
        key="daily-sync",
        ttl=timedelta(minutes=30),
    )
except LockAcquisitionError:
    # Another process is already doing this work.
    return

try:
    run_sync()
    lock.refresh()  # Optional, for long-running work.
finally:
    lock.release()
```

## Notes And Limitations

- Lock cleanup is lazy. Expired rows remain in the table until the next acquisition for the same `key`, at which point the row is reused.
- There is no context-manager helper yet; callers must handle `release()` themselves.
