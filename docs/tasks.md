# Tasks

The `synch.tasks` and `edrweb.tasks` modules define the Celery tasks used by
the synchronization apps.

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
- It runs `sync_changed_patient_phone_numbers_to_turn` sixth.
- It only proceeds to the next step if the previous step completed successfully.
- It wraps the sync steps in a database transaction, so a failure in any step rolls back the local database updates made during that run.
- If it cannot get the top-level lock, it logs a warning and does not attempt any sync or Turn import.

### Benchmarking `sync_all`

Use `benchmark_sync_all` to run `sync_all` against generated benchmark data with
mocked CCMDD and Turn clients:

```bash
uv run ./manage.py benchmark_sync_all
```

The default scale mirrors the latest complete production run captured in
`sync_all_logs.txt`: 10,731 facilities, 11,161 patients, 11,555 existing
prescriptions, 25 prescription updates, 10 patient-row updates, 24 new Turn
patient imports, and 1 phone-number change. The logs had 0 phone-number changes,
but the benchmark keeps one by default so that path is exercised.

The command deletes previous benchmark-owned rows at start, seeds fresh data,
runs the real `synch.tasks.sync_all` code path, and prints total runtime plus
per-step timings. Each step also reports total database query count, total
database time, and the five slowest query groups by cumulative database time, so
slow steps can be tied back to repeated query patterns. It leaves benchmark rows
in place for inspection unless `--cleanup` is passed.

For accurate results, run it against a dedicated local benchmark database. By
default it refuses to run when non-benchmark `synch` data exists; pass
`--allow-existing-data` only when mixed data is intentional.

## `sync_appointment_reminder_delta`

`edrweb.tasks.sync_appointment_reminder_delta` is the scheduled delta task for
the EDRWeb Appointment Reminder Feed.

- Celery Beat schedules it to run every 4 hours.
- It skips without acquiring the lock when `EDRWEB_BASE_URL`, `EDRWEB_USERNAME`,
  or `EDRWEB_PASSWORD` is not configured.
- It acquires the shared `sync-edrweb-appointment-reminders` lock before
  starting, so a delta pull and full reconciliation pull cannot overlap.
- It derives the delta checkpoint from the latest stored `EDRWebPatient.updated_at`
  value.
- If no EDRWeb patient snapshots exist locally, it omits `updatedSince`.
- If a checkpoint exists, it queries from one second before that checkpoint to
  avoid missing same-timestamp upstream updates.
- It stores one current local `EDRWebPatient` snapshot per EDRWeb `PersonId`.
- For each returned record, it stores `PersonId`, `PhoneNumber`, `UpdatedAt`, and
  `Appointments` in explicit model fields.
- It stores every remaining EDRWeb patient field in the `EDRWebPatient.payload`
  JSON column.
- Missing `PhoneNumber` is stored as a blank string.
- Missing `Appointments` is stored as an empty list.
- A returned record marks the stored EDRWeb patient snapshot as active and clears
  `feed_removed_at`, even if the returned record is older than the stored
  snapshot.
- It rejects records without `PersonId` or timezone-aware `UpdatedAt`, and records
  whose `Appointments` value is not a list.
- It ignores incoming records that are older than the currently stored snapshot
  for the same `PersonId`.
- It wraps the pull and upserts in a database transaction, so a failure rolls back
  all local database updates made during that run.
- After the local transaction commits, it calls `sync_appointment_reminders_to_turn`
  while still holding the EDRWeb appointment reminder lock.
- If the appointment reminder Turn import succeeds, it calls
  `sync_messaging_contact_activations_to_turn` while still holding the same lock.
- It then calls `sync_changed_patient_phone_numbers_to_turn` while still holding
  the same lock.
- If the Turn import fails, the completed local EDRWeb snapshot changes remain
  committed, EDRWeb activation and phone-change handling are not attempted, and
  a later run can retry Turn from the local snapshots.
- If it cannot get the lock, it logs a warning and does not attempt any API pull.

## `sync_appointment_reminder_full_reconciliation`

`edrweb.tasks.sync_appointment_reminder_full_reconciliation` is the scheduled
full reconciliation task for the EDRWeb Appointment Reminder Feed.

- Celery Beat schedules it to run weekly on Monday at 02:00 UTC.
- It skips without acquiring the lock when `EDRWEB_BASE_URL`, `EDRWEB_USERNAME`,
  or `EDRWEB_PASSWORD` is not configured.
- It acquires the shared `sync-edrweb-appointment-reminders` lock before
  starting, so a full reconciliation pull and delta pull cannot overlap.
- If the shared lock is already held, it retries after 15 minutes instead of
  skipping the run.
- It omits `updatedSince`, so the EDRWeb API returns the current full
  appointment reminder feed.
- It stores each returned record through the same snapshot upsert rules as the
  delta task.
- After the full feed has completed successfully, it marks active local
  `EDRWebPatient` snapshots that were absent from the full feed as inactive and
  stores `feed_removed_at` using Bifrost processing time.
- It wraps the pull, upserts, and inactive marking in a database transaction, so
  a failure rolls back all local database updates made during that run.
- After the local transaction commits, it calls `sync_appointment_reminders_to_turn`
  while still holding the EDRWeb appointment reminder lock.
- If the appointment reminder Turn import succeeds, it calls
  `sync_messaging_contact_activations_to_turn` while still holding the same lock.
- It then calls `sync_changed_patient_phone_numbers_to_turn` while still holding
  the same lock.
- If the Turn import fails, the completed local EDRWeb snapshot changes remain
  committed, EDRWeb activation and phone-change handling are not attempted, and
  a later run can retry Turn from the local snapshots.

## `sync_appointment_reminders_to_turn`

`edrweb.tasks.sync_appointment_reminders_to_turn` refreshes EDRWeb appointment
reminder contact fields in Turn from locally stored `EDRWebPatient` snapshots.

- It is called after each completed EDRWeb delta pull and full reconciliation
  pull.
- It iterates all locally stored `EDRWebPatient` rows, not only rows touched by
  the latest pull, so earlier Turn failures and admin corrections are retried.
- It normalizes `EDRWebPatient.phone_number` to E.164 with `phonenumbers`,
  assuming South Africa (`ZA`) when no country code is provided.
- It skips EDRWeb patients whose phone number cannot be normalized.
- For active EDRWeb patients, it sends `edrweb_patient_id`, blank appointment
  context fields, and then fills appointment context from the earliest valid
  `AppointmentDate` when one exists.
- EDRWeb appointment facility fields come from the selected appointment's
  `Facility` object. `FacilityName`, `Latitude`, and `Longitude` are left blank
  when absent.
- It does not set `edrweb_reminders` to `True` and does not set
  `edrweb_new_user`; `sync_messaging_contact_activations_to_turn` owns that
  welcome-message trigger.
- For inactive EDRWeb patients, it sends only `urn` and
  `edrweb_reminders = "False"`, leaving historical EDRWeb context fields intact.
- It sends the rows through the Turn CSV contacts import API.
- It raises an error if Turn reports row-level import errors in the API response.

## `sync_messaging_contact_activations_to_turn`

`edrweb.tasks.sync_messaging_contact_activations_to_turn` imports the
`edrweb_new_user` contact field into Turn for EDRWeb patients whose welcome
message has not yet been triggered.

- It runs only after `sync_appointment_reminders_to_turn` succeeds, so the Turn
  contact already has current EDRWeb patient and appointment context.
- It filters `EDRWebPatient` records to active rows where
  `messaging_contact_activated` is `False`.
- It normalizes `EDRWebPatient.phone_number` to E.164 with `phonenumbers`,
  assuming South Africa (`ZA`) when no country code is provided.
- It skips EDRWeb patients whose phone number cannot be normalized.
- It does not require appointment or facility context.
- It sets `edrweb_new_user` to a single `timezone.now().isoformat()` value
  generated once for the batch.
- It does not directly set `edrweb_reminders` to `True`; the welcome-message
  activation flow owns reminder enabling.
- It logs Turn row-level import errors and does not advance those EDRWeb
  patients' stored active messaging phone number.
- It updates `messaging_contact_activated` to `True` for all successfully
  imported EDRWeb patients.
- It stores the normalized current phone number in `active_messaging_phone_number`
  for all successfully imported EDRWeb patients.

## `sync_changed_patient_phone_numbers_to_turn`

`edrweb.tasks.sync_changed_patient_phone_numbers_to_turn` handles EDRWeb patients
whose current phone number differs from the Turn contact Bifrost last activated
for EDRWeb messaging.

- It runs after `sync_appointment_reminders_to_turn`, so the new Turn contact
  already has current EDRWeb patient and appointment context before
  `edrweb_new_user` is set.
- It filters to active `EDRWebPatient` rows where
  `messaging_contact_activated` is `True` and `active_messaging_phone_number` is
  not blank.
- It uses the stored normalized phone numbers directly, so
  `active_messaging_phone_number` and `phone_number` are compared in the
  database without extra normalization.
- It loads the matching patients in one ordered queryset list, then refreshes
  the lock before any Turn API work begins.
- For each changed phone number, it first imports a retirement row for the old
  Turn contact with `edrweb_reminders` set to `False`.
- It refreshes the lock after each Turn API call.
- It imports an `edrweb_new_user` trigger row for the new Turn contact only when
  the old-contact retirement row succeeded.
- It uses one `timezone.now().isoformat()` trigger timestamp for the task run.
- It stores the new active messaging phone number only for EDRWeb patients whose
  old-contact retirement and new-contact trigger rows both succeeded.
- Failed rows remain retryable in later sync runs because the stored active
  messaging phone number is not advanced.

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
- It uses the tracked appointment's facility when an unresolved appointment
  resolves to a `Facility` with a non-blank name and is not yet missed.
- If no usable tracked appointment exists after filtering by facility validity,
  related prescriptions, and missed-appointment windows, it falls back to the
  most recently created prescription whose facility resolves to a non-blank name.
- It sets `synch_new_user` to a single `timezone.now().isoformat()` value generated once for the batch.
- It sends the rows through the Turn CSV contacts import API.
- It logs Turn row-level import errors and does not mark those patients as imported.
- It updates `invite_sent` to `True` and stores the active messaging phone number for all successfully imported patients.

## `sync_changed_patient_phone_numbers_to_turn`

`synch.tasks.sync_changed_patient_phone_numbers_to_turn` handles patients whose current messaging phone number differs from the Turn contact Bifrost last activated for them.

- It runs after `sync_appointment_dates_to_turn`, so the new Turn contact already has current patient and appointment context before `synch_new_user` is set.
- It runs after `sync_new_patients_to_turn`, so first-time imports establish their active messaging phone number before phone-change handling.
- Patients invited before active-contact tracking existed are backfilled by the migration that adds the stored active messaging phone number.
- It skips patients with no replacement messaging phone number.
- It compares stored and current phone numbers using normalized E.164 values.
- For each changed phone number, it first imports a retirement row for the old Turn contact with `synch_reminders` set to `False`.
- It imports a `synch_new_user` trigger row for the new Turn contact only when the old-contact retirement row succeeded.
- It uses one `timezone.now().isoformat()` trigger timestamp for the task run.
- It stores the new active messaging phone number only for patients whose old-contact retirement and new-contact trigger rows both succeeded.
- Failed rows remain retryable in later sync runs because their stored active messaging phone number is not advanced.

## `sync_appointment_dates_to_turn`

`synch.tasks.sync_appointment_dates_to_turn` refreshes next-appointment contact fields in Turn for every locally synced patient.
See [SyNCH Appointment Reminder Logic](./synch-appointment-reminders.md) for
the full appointment and missed-appointment reminder rules.

- It iterates all `Patient` records in the local database.
- For each patient, it resolves the same shared patient messaging state used by `sync_new_patients_to_turn`.
- It uses the most recent valid prescription `patient_phone` as the Turn `urn`, normalized to E.164 with `phonenumbers` and assuming South Africa (`ZA`) when no country code is provided.
- It skips patients that have no usable messaging phone number.
- It sends `synch_patient_id` as the raw CCMDD patient identifier for every emitted row.
- It flattens `return_dates` across all of the patient's prescriptions,
  discards appointments whose facility is missing or unnamed, discards
  appointments resolved by a related prescription, discards missed
  appointments after their eight-week post-appointment window ends, and selects
  the earliest remaining appointment.
- It treats a prescription as related to an appointment when that prescription
  was created from two weeks before through eight weeks after the appointment
  date, inclusive.
- It does not allow a prescription to resolve an appointment date carried by
  that same prescription.
- If multiple usable appointments share the same earliest date, it selects the appointment from the most recently created prescription.
- It sends `synch_next_appointment_date` only when a usable tracked appointment exists.
- It sends `synch_appointment_facility_name`, `synch_appointment_facility_latitude`, and `synch_appointment_facility_longitude` from the selected appointment's facility when a usable tracked appointment exists.
- If no usable tracked appointment exists after filtering by facility validity,
  related prescriptions, and missed-appointment windows, it falls back to the
  most recently created prescription whose facility resolves to a non-blank name
  and sends those facility fields while leaving `synch_next_appointment_date`
  blank.
- It sends the rows through the Turn CSV contacts import API.
- It raises an error if Turn reports row-level import errors in the API response.
