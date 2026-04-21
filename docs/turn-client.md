# Turn Client

The sync app includes a small HTTP client for Turn's CSV contacts import API in `synch/turn.py`.

## Scope

The client supports one Turn endpoint:

- `POST /v1/contacts`

It is intended for bulk contact create-or-update imports via CSV.

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

The CCMDD sync task uses this import path to update Turn contacts with `urn` and the `synch_new_user` contact field for newly created patients.

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
