# Django Admin

Bifrost uses the Django admin for inspecting and correcting local sync
snapshots. Admin edits change local Bifrost state only; they do not write back
to SyNCH CCMDD, EDRWeb, or Turn.

## CCMDD Admin

The `synch` app registers the local CCMDD snapshot models.

### Patients

`Patient` rows are local snapshots of SyNCH CCMDD patients.

- Listed fields: `ccmdd_patient_id`, `date_created`, `date_updated`.
- Search fields: `ccmdd_patient_id`.
- `payload` is read-only because it stores residual upstream CCMDD patient
  fields that were not promoted into explicit model columns.

### Prescriptions

`Prescription` rows are local snapshots of SyNCH CCMDD prescriptions. They
connect patients to prescription dates, phone numbers, department data, and
return dates used by appointment and messaging tasks.

- Listed fields: `ccmdd_prescription_id`, `patient_id`, `date_created`,
  `date_updated`.
- Search fields: `ccmdd_prescription_id`, `patient_id`.
- `payload` is read-only because it stores residual upstream CCMDD prescription
  fields that were not promoted into explicit model columns.
- `return_dates` is editable and must be a JSON list of objects.
- Each `return_dates` entry may contain only `return_date`, `note`, `day_count`,
  and `description`.
- Each `return_dates` entry must include `return_date` as `YYYY-MM-DD`.

### Facilities

`Facility` rows are local snapshots of CCMDD service locations used when
Bifrost resolves appointment facility context for Turn messaging.

- Listed fields: `ccmdd_facility_id`, `name`, `telephone`.
- Search fields: `ccmdd_facility_id`, `name`.
- `payload` is read-only because it stores residual upstream CCMDD facility
  fields that were not promoted into explicit model columns.

## EDRWeb Admin

The `edrweb` app registers `EDRWebPatient` so support staff can inspect and
correct local EDRWeb appointment reminder snapshots.

- Listed fields: `patient_id`, `phone_number`, `updated_at`, `is_active`,
  `feed_removed_at`.
- Search fields: `patient_id`, `phone_number`.
- Editable fields: `patient_id`, `phone_number`, `updated_at`, `is_active`,
  and `appointments`.
- `payload` is read-only because it stores residual upstream EDRWeb fields that
  were not promoted into explicit model columns.
- `feed_removed_at` is read-only. Deactivating an EDRWeb patient sets it to the
  current Bifrost processing time; reactivating the patient clears it.
- `updated_at` cannot be set in the future because Bifrost uses the latest
  stored value as the next EDRWeb delta checkpoint.
- `appointments` must be a JSON list of objects. Each appointment must include
  `AppointmentDate` in `YYYY-MM-DD` format and may include `Facility` with
  `FacilityName`, `Latitude`, and `Longitude`.
- The model field help text documents each field's source and messaging use so
  manual admin edits have local guidance.
