# CCMDD Client

The sync app includes a small HTTP client for [SyNCH's CCMDD API](https://test.ccmdd.org.za/wapi) in synch/ccmdd.py

## Scope

The client only supports these CCMDD endpoints:

- `POST /wapi/prescriptionLimited`
- `POST /wapi/patientLimited`

For both endpoints, the only supported filter is the optional `date_updated` field.

## Usage

Instantiate the client with the CCMDD base URL and digest-auth credentials:

```python
from synch.ccmdd import CCMDDAPIClient

client = CCMDDAPIClient(
    base_url="https://test.ccmdd.org.za",
    username="api-user",
    password="secret",
)
```

Fetch updated prescriptions or patients:

```python
from datetime import datetime

for prescription in client.iter_limited_prescriptions():
    ...

for patient in client.iter_limited_patients(
    date_updated=datetime(2024, 1, 2, 3, 4, 5),
):
    ...
```

The methods return iterators of raw item dictionaries from the CCMDD API response.

## `date_updated`

`date_updated` is optional for both methods.

- If omitted, the request body is empty.
- If provided, it is formatted as `YYYY-MM-DD HH:MM:SS.SSSSSS`.

## Long-running operations

The client handles the CCMDD long-running operation flow automatically.

- For `202 Accepted` responses, it follows the returned `status_location` until the operation succeeds.
- It waits 5 minutes between each poll of the status endpoint.
- It gives up after 12 status polls, which is a 1 hour wait budget.
- Temporary retries due to failures while polling do not count against the 12 poll limit.
- Once the status is `succeeded`, it fetches the final data from `resource_location`.
- Multi-operation patient responses are fetched and flattened into one iterator.

## Retries

The client retries temporary failures up to 5 times with random exponential backoff.

- Retryable `4xx` statuses: `408`, `409`, `425`, `429`
- Retryable `5xx` statuses: all `500`, `502`, `503`, `504`
- Retryable transport failures: `requests` exceptions such as timeouts and connection errors

Non-temporary failures raise an exception immediately.

## Exceptions

The client raises `CCMDDAPIError` subclasses for caller-visible failures:

- `CCMDDRetryExhausted`: the request kept failing with temporary errors until the retry limit was exceeded
- `CCMDDLongRunningOperationTimeout`: the operation status did not reach `succeeded` within 12 polls
