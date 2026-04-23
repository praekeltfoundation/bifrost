# Tasks

The `synch.tasks` module defines the Celery tasks used by the synchronization app.

## `healthcheck`

`synch.tasks.healthcheck` is a small shared task that returns `"OK"`.

It exists as a minimal Celery execution check so the project can verify that:

- Celery task autodiscovery is working
- a worker can execute a shared task

## `sync_all`

`synch.tasks.sync_all` is the scheduled top-level task for CCMDD synchronization.

- Celery Beat schedules it to run every 5 minutes.
- It acquires the `sync-ccmdd` lock before starting, so only one full CCMDD sync run can proceed at a time.
- It runs `sync_patients` first.
- It runs `sync_facilities` second.
- It runs `sync_prescriptions` third.
- It runs `sync_appointment_dates_to_turn` fourth.
- It runs `sync_new_patients_to_turn` fifth.
- It only proceeds to the next step if the previous step completed successfully.
- If it cannot get the top-level lock, it logs a warning and does not attempt any sync or Turn import.

## `sync_patients`

`synch.tasks.sync_patients` incrementally synchronizes patients from the CCMDD API into the local database.

- It reads the latest stored patient `date_updated` value from the local database, defaulting to the Unix epoch if there are no patients yet, as it is a required field for the API.
- It calls `iter_limited_patients(date_updated=...)` to fetch updates from the CCMDD API.
- For each returned patient, it stores `id`, `date_created`, and `date_updated` in explicit model fields, and stores the remaining CCMDD patient fields in the `Patient.payload` JSON column.
- If a patient already exists, it is updated instead of a new one being created.
- It returns the pre-sync patient `date_updated` watermark so the top-level task can identify which patients were newly created during the run.

## `sync_prescriptions`

`synch.tasks.sync_prescriptions` incrementally synchronizes prescriptions from the CCMDD API into the local database.

- It reads the latest stored prescription `date_updated` value from the local database, defaulting to the Unix epoch if there are no prescriptions yet.
- It calls `iter_limited_prescriptions(date_updated=...)` to fetch updates from the CCMDD API.
- For each returned prescription, it stores `id`, `date_created`, `date_updated`, `facility_id`, `patient_id`, `patient_phone`, `department_id`, and `return_dates` in explicit model fields.
- It stores every remaining CCMDD prescription field in the `Prescription.payload` JSON column.
- If a prescription already exists, it is updated instead of a new one being created.

## `sync_facilities`

`synch.tasks.sync_facilities` synchronizes the full facility list from the CCMDD API
into the local database.

- It calls `iter_facilities()` to fetch all facilities from the CCMDD API.
- For each returned facility, it stores `id`, `level_desc_5`, `latitude`,
  `longitude`, `telephone`, `address_1`, and `address_2` in explicit model fields.
- It stores every remaining CCMDD facility field in the `Facility.payload` JSON column.
- It bulk upserts the full facility list so existing facilities are updated and new
  facilities are created in one database write.

## `sync_new_patients_to_turn`

`synch.tasks.sync_new_patients_to_turn` imports the `synch_new_user` contact field into Turn for patients created during the current sync run.

- It filters `Patient` records to only those with `date_created` later than last sync date.
- For each qualifying patient, it finds the most recent `Prescription` by `date_created` for the matching `patient_id`.
- It normalizes that prescription's `patient_phone` to E.164 with `phonenumbers` before using it as the Turn `urn`, assuming South Africa (`ZA`) when no country code is provided.
- It skips patients that have no prescriptions, whose latest prescription has a blank `patient_phone`, or whose phone number cannot be parsed well enough to format.
- It sets `synch_new_user` to a single `timezone.now().isoformat()` value generated once for the batch.
- It sends the rows through the Turn CSV contacts import API.
- It raises an error if Turn reports row-level import errors in the API response.

## `sync_appointment_dates_to_turn`

`synch.tasks.sync_appointment_dates_to_turn` refreshes next-appointment contact fields in Turn for every locally synced patient.

- It iterates all `Patient` records in the local database.
- For each patient, it fetches all matching `Prescription` records and uses the latest prescription by `date_created` to source the Turn `urn` from `patient_phone`.
- It normalizes that phone number to E.164 with `phonenumbers`, assuming South Africa (`ZA`) when no country code is provided.
- It skips patients that have no prescriptions, whose latest prescription has a blank `patient_phone`, or whose phone number cannot be parsed well enough to format.
- It flattens `return_dates` across all of the patient's prescriptions, keeps only appointment dates on or after `django.utils.timezone.localdate()`, sorts them, and selects the earliest upcoming appointment.
- It looks up the `Facility` matching the selected appointment's prescription `facility_id` and sends `synch_appointment_facility_name`, `synch_appointment_facility_latitude`, and `synch_appointment_facility_longitude` when available.
- It clears `synch_next_appointment_date`, `synch_appointment_facility_name`, `synch_appointment_facility_latitude`, and `synch_appointment_facility_longitude` with empty strings when a patient has no appointment on or after today.
- It sends the rows through the Turn CSV contacts import API.
- It raises an error if Turn reports row-level import errors in the API response.
