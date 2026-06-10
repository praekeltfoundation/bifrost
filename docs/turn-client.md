# Turn Client

The sync app includes a small HTTP client for Turn's contact and message APIs in `synch/turn.py`.

## Scope

The client supports three Turn endpoints:

- `POST /v1/contacts`
- `PATCH /v1/contacts/<contact_id>/profile`
- `POST /v1/messages`

It is intended for bulk contact create-or-update imports via CSV plus a narrow set of one-off contact profile updates and template sends.

## Usage

Instantiate the client with the Turn base URL and bearer token:

```python
from synch.turn import TurnAPIClient

client = TurnAPIClient(
    base_url="https://whatsapp.turn.io",
    token="your-access-token",
)
```

Import contacts from a list of dictionaries:

```python
errors = client.import_contacts(
    [
        {"urn": "+27123456789", "name": "Peter"},
        {"urn": "+27123456790", "surname": "Parker", "opted_in": "true"},
    ]
)
```

The method returns a list of parsed error rows from Turn's streamed CSV response. Successful rows are ignored.

The CCMDD sync tasks use this import path to update Turn contacts with a shared patient messaging phone number, the `synch_patient_id` link field, appointment contact fields, and the `synch_new_user` contact field for invite-eligible patients.

The EDRWeb sync tasks use this import path after each completed EDRWeb pull to
refresh `edrweb_patient_id` and EDRWeb appointment contact fields, or to set
`edrweb_reminders` to `"False"` for inactive EDRWeb patients.

Update a single contact profile:

```python
client.update_contact_profile(
    contact_id="27123456789",
    fields={
        "sync_reminders": "True",
        "contact_ndoh_privacy_policy": "true",
    },
)
```

Send a templated message:

```python
message_id = client.send_template_message(
    msisdn="+27123456789",
    template_namespace="namespace",
    template_name="template_name",
    template_language="en",
    body_parameters=["Facility Name"],
)
```

## Input shape

- The client accepts `list[dict[str, object]]`.
- The CSV columns are the union of all keys in first-seen order.
- Missing values are emitted as empty cells.
- The overall input must include an `urn` column.

## Batching

Turn limits CSV imports to 1 MB per request.

- The client automatically splits requests into batches that stay within the 1 MB limit.
- Every batch includes its own header row.
- If a single row cannot fit into a batch together with the header row, the client raises `TurnRowTooLargeError`.

## Retries

The client retries temporary failures up to 5 times using random exponential backoff.

- Retryable `4xx` statuses: `408`, `409`, `425`, `429`
- Retryable `5xx` statuses: `500`, `502`, `503`, `504`
- Retryable transport failures: `requests` exceptions such as timeouts and connection errors

For `429` responses, `Retry-After` is treated as a minimum delay. The actual sleep duration is the larger of `Retry-After` and the exponential backoff delay.

## Exceptions

The client raises `TurnAPIError` subclasses for caller-visible failures:

- `TurnRetryExhausted`: the request kept failing with temporary errors until the retry limit was exceeded
- `TurnRowTooLargeError`: a single contact row is too large to fit into one CSV batch

## Local Consent Backfill

The repo includes a local-only management command for a one-off Turn reminder consent backfill:

```bash
uv run ./manage.py backfill_turn_consent /path/to/export.csv
uv run ./manage.py backfill_turn_consent /path/to/export.csv --execute
```

- Without `--execute`, the command runs in preview mode and prints what it would do.
- With `--execute`, it updates each Turn contact's `sync_reminders` and `contact_ndoh_privacy_policy` fields, then sends the hardcoded `synch_service_confirmation_6` template using the CSV `synch_appointment_facility_name` as the only body parameter.
- It writes an outcome ledger next to the input CSV by default using the suffix `.turn_consent_backfill.csv`.
- On rerun, rows already marked `success` in the ledger are skipped.
