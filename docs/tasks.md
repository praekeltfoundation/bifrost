# Tasks

The `synch.tasks` module defines the Celery tasks used by the synchronization app.

## `healthcheck`

`synch.tasks.healthcheck` is a small shared task that returns `"OK"`.

It exists as a minimal Celery execution check so the project can verify that:

- Celery task autodiscovery is working
- a worker can execute a shared task

## `sync_all`

`synch.tasks.sync_all` is the scheduled top-level task for CCMDD synchronization.

- It acquires the `sync-ccmdd` lock before starting, so only one full CCMDD sync run can proceed at a time.
- It runs `sync_patients` first.
- It only runs `sync_prescriptions` if the patient sync completes successfully.
- If it cannot get the top-level lock, it logs a warning and does not attempt either sync.

## `sync_patients`

`synch.tasks.sync_patients` incrementally synchronizes patients from the CCMDD API into the local database.

- It reads the latest stored patient `date_updated` value from the local database, defaulting to the Unix epoch if there are no patients yet, as it is a required field for the API.
- It calls `iter_limited_patients(date_updated=...)` to fetch updates from the CCMDD API.
- For each returned patient, it stores `id`, `date_created`, and `date_updated` in explicit model fields, and stores the remaining CCMDD patient fields in the `Patient.payload` JSON column.
- If a patient already exists, it is updated instead of a new one being created.

## `sync_prescriptions`

`synch.tasks.sync_prescriptions` incrementally synchronizes prescriptions from the CCMDD API into the local database.

- It reads the latest stored prescription `date_updated` value from the local database, defaulting to the Unix epoch if there are no prescriptions yet.
- It calls `iter_limited_prescriptions(date_updated=...)` to fetch updates from the CCMDD API.
- For each returned prescription, it stores `id`, `date_created`, `date_updated`, `facility_id`, `patient_id`, `patient_phone`, `department_id`, and `return_dates` in explicit model fields.
- It stores every remaining CCMDD prescription field in the `Prescription.payload` JSON column.
- If a prescription already exists, it is updated instead of a new one being created.
