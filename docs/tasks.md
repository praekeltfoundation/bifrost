# Tasks

The `synch.tasks` module defines the Celery tasks used by the synchronization app.

## `healthcheck`

`synch.tasks.healthcheck` is a small shared task that returns `"OK"`.

It exists as a minimal Celery execution check so the project can verify that:

- Celery task autodiscovery is working
- a worker can execute a shared task

## `sync_patients`

`synch.tasks.sync_patients` incrementally synchronizes patients from the CCMDD API into the local database.

- It acquires the `sync-patients` lock before starting, so only one patient sync runs at a time. If it cannot get a lock, it logs a warning and ends.
- It reads the latest stored patient `date_updated` value from the local database, defaulting to the Unix epoch if there are no patients yet, as it is a required field for the API.
- It calls `iter_limited_patients(date_updated=...)` to fetch updates from the CCMDD API.
- For each returned patient, it stores `id`, `date_created`, and `date_updated` in explicit model fields, and stores the remaining CCMDD patient fields in the `Patient.payload` JSON column.
- If a patient already exists, it is updated instead of a new one being created.
