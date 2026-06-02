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
- It runs `sync_facilities` first.
- It captures the current prescription `date_updated` watermark before syncing prescriptions.
- It runs `sync_prescriptions` second with that captured watermark.
- It runs `sync_patients` third with the same captured prescription watermark.
- It runs `sync_appointment_dates_to_turn` fourth.
- It runs `sync_new_patients_to_turn` fifth.
- It only proceeds to the next step if the previous step completed successfully.
- It wraps the sync steps in a database transaction, so a failure in any step rolls back the local database updates made during that run.
- If it cannot get the top-level lock, it logs a warning and does not attempt any sync or Turn import.

## `sync_patients`

`synch.tasks.sync_patients` incrementally synchronizes patients from the CCMDD API into the local database.

- The CCMDD patient endpoint is pre-filtered upstream by a relevant-prescription rule, so patient completeness depends on both patient-row updates and prescription-row updates.
- It reads the latest stored patient `date_updated` value from the local database, defaulting to the Unix epoch if there are no patients yet.
- It calls `iter_limited_patients(date_updated=...)` to fetch patients whose own rows changed.
- It calls `iter_limited_patients(prescription_date_updated=...)` with the pre-run prescription watermark from `sync_all` to fetch patients whose relevant prescriptions changed, even if the patient row did not.
- For each returned patient, it stores `id`, `date_created`, and `date_updated` in explicit model fields, and stores the remaining CCMDD patient fields in the `Patient.payload` JSON column.
- If a patient already exists, it is updated instead of a new one being created.
- It allows inclusive watermark re-fetches and relies on idempotent upserts instead of trying to skip same-timestamp rows.
- It logs separate counts for patient-row updates and prescription-triggered patient refreshes, plus a combined total.

## `sync_prescriptions`

`synch.tasks.sync_prescriptions` incrementally synchronizes prescriptions from the CCMDD API into the local database.

- It receives the pre-run prescription `date_updated` watermark from `sync_all`.
- It calls `iter_limited_prescriptions(date_updated=...)` with that captured watermark to fetch the exact prescription window that the paired patient refresh will also use.
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

`synch.tasks.sync_new_patients_to_turn` imports the `synch_new_user` contact field into Turn for patients who haven't yet been sent the invite.

- It filters `Patient` records to only those with `invite_sent` as `False`.
- For each qualifying patient, it resolves a shared patient messaging state from all matching prescriptions.
- It uses the most recent valid prescription `patient_phone` as the Turn `urn`, normalized to E.164 with `phonenumbers` and assuming South Africa (`ZA`) when no country code is provided.
- It only imports patients that have both a usable messaging phone number and a usable facility.
- It uses the upcoming appointment's facility when a future appointment on or after `django.utils.timezone.localdate()` resolves to a `Facility` with a non-blank name.
- If no usable upcoming appointment exists after filtering by date and facility validity, it falls back to the most recently created prescription whose facility resolves to a non-blank name.
- It sets `synch_new_user` to a single `timezone.now().isoformat()` value generated once for the batch.
- It sends the rows through the Turn CSV contacts import API.
- It raises an error if Turn reports row-level import errors in the API response.
- It updates `invite_sent` to `True` for all successfully imported patients.

## `sync_appointment_dates_to_turn`

`synch.tasks.sync_appointment_dates_to_turn` refreshes next-appointment contact fields in Turn for every locally synced patient.

- It iterates all `Patient` records in the local database.
- For each patient, it resolves the same shared patient messaging state used by `sync_new_patients_to_turn`.
- It uses the most recent valid prescription `patient_phone` as the Turn `urn`, normalized to E.164 with `phonenumbers` and assuming South Africa (`ZA`) when no country code is provided.
- It skips patients that have no usable messaging phone number.
- It sends `synch_patient_id` as the raw CCMDD patient identifier for every emitted row.
- It flattens `return_dates` across all of the patient's prescriptions, keeps only appointment dates on or after `django.utils.timezone.localdate()`, discards appointments whose facility is missing or unnamed, and selects the earliest remaining appointment.
- If multiple usable appointments share the same earliest date, it selects the appointment from the most recently created prescription.
- It sends `synch_next_appointment_date` only when a usable upcoming appointment exists.
- It sends `synch_appointment_facility_name`, `synch_appointment_facility_latitude`, and `synch_appointment_facility_longitude` from the selected appointment's facility when a usable upcoming appointment exists.
- If no usable upcoming appointment exists after filtering by date and facility validity, it falls back to the most recently created prescription whose facility resolves to a non-blank name and sends those facility fields while leaving `synch_next_appointment_date` blank.
- It sends the rows through the Turn CSV contacts import API.
- It raises an error if Turn reports row-level import errors in the API response.
