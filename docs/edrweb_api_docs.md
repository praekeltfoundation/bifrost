# EDRWeb Integration API Guide 

**Software Product Name:** EDRWeb 
**Author:** WAMTech Systems (Pty) Ltd 
**Version:** 1.0.0 
**Date:** 02/06/2026 
**Department:** Department of Health, Republic of South Africa 

---

## 1. Overview

* The EDRWeb Integration API provides authorized third-party systems with structured access to EDRWeb data.
* Access is scoped per provider account; each account is restricted to the data agreed upon with WAMTech Systems.
* This guide covers the Appointment Reminders capability, which provides a read-only feed of DR-TB patients who have explicitly consented to WhatsApp appointment reminders.
* Each record includes the patient's phone number and their scheduled milestone appointment dates at their active treatment facility.
* An OpenAPI 3.0 specification (`EDRWeb-Integration-API.yml`) accompanies this document and is the authoritative source for schema definitions.

---

## 2. Base URLs

* All communication must occur over HTTPS.
* Plain HTTP is not supported.

| Environment | Base URL |
| --- | --- |
| **QA/Staging** | `https://staging.edrweb.net/api` |
| **Production** | `https://edrweb.net/api` |

---

## 3. Authentication

### Mechanism 

* The API uses JWT Bearer token authentication.
* All requests to protected endpoints must include the following header:

`Authorization: Bearer <access_token>` 

### Credential Exchange 

* Credentials (username and password) are issued per-environment by WAMTech Systems.
* Credentials will be delivered through an encrypted channel prior to integration.

### Obtaining a Token 

* 
**Endpoint:** `POST /auth/token` 

* **Request Body:**
```json
{
  "username": "provided_username",
  "password": "........"
}

```

* 
**Response (200 OK):** 

```json
{
  "AccessToken": "eyJhbGciOiJSUzI1NilsInR5cCl6lkpXVCJ9...",
  "Type": "Bearer",
  "ExpiresAt": "2026-06-02T08:00:00.000+00:00",
  "RefreshToken": "dGhpcyBpcyBhIHJlZnJlc2ggdG9rZW4...",
  "RefreshExpiresAt": "2026-06-09T06:00:00.000+00:00"
}
```

* The access token is valid for approximately 1 hour, while the refresh token is valid for 7 days.

### Refreshing a Token 

* **Endpoint:** `POST /auth/token/refresh` 

* **Request Body:**

```json
{
  "RefreshToken": "dGhpcyBpcyBhIHJlZnJlc2ggdG9rZW4..."
}

```

* This endpoint returns a new Token Response with a fresh access token and refresh token.

* Implement a proactive refresh loop before the access token expires to avoid disruption.

---

## 4. Endpoint

`GET /persons/appointment-reminders` 

* Returns a paginated list of consenting patients with their scheduled appointments.

Query Parameters 

| Parameter | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `updatedSince` | ISO-8601 datetime with offset | No |  | Delta sync filter. Only records changed after this timestamp are returned. Omit for a full sync. |
| `cursor` | string (opaque) | No |  | Pagination cursor from the previous response's `nextCursor`. Omit on first request. |
| `pageSize` | integer (1-500) | No | 100 | Number of patient records per page. |
| `upcomingOnly` | boolean | No | true | When true, only future appointment dates are returned. Set to false to include past dates. |

Example Request - Full Sync, First Page 

```
GET /persons/appointment-reminders?pageSize=200
Authorization: Bearer eyJhbGciOiJSUzI1NilsInR5cCl6lkpXVCJ9...
```

Example Request - Delta Sync 

```
GET /persons/appointment-reminders?updatedSince=2026-06-01T00:00:00%2B02:00
Authorization: Bearer eyJhbGciOiJSUzI1NilsInR5cCI6IkpXVCJ9...
```

Example Request - Next Page 

```
GET /persons/appointment-reminders?cursor=MTIzNDU%3D&pageSize=200
Authorization: Bearer eyJhbGciOiJSUzI1NilsInR5cCI6IkpXVCJ9...
```

Example Response (200 OK) 

> 
> **Note on Null Serialization:** Null fields are omitted from the response entirely. A field absent from the response should be treated as null by the consumer.
> 
> 

```json
{
  "Persons": [
    {
      "PersonId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
      "PhoneNumber": "+27721234567",
      "UpdatedAt": "2026-05-30T14:22:00.000+02:00",
      "Appointments": [
        {
          "AppointmentDate": "2026-06-20",
          "Facility": {
            "FacilityName": "WC BLUE DOWNS CLINIC",
            "Latitude": -33.9744,
            "Longitude": 18.7032
          }
        },
        {
          "AppointmentDate": "2026-07-20",
          "Facility": {
            "FacilityName": "WC BLUE DOWNS CLINIC",
            "Latitude": -33.9744,
            "Longitude": 18.7032
          }
        }
      ]
    },
    {
      "PersonId": "b2c3d4e5-f6a7-8901-bcde-f12345678901",
      "PhoneNumber": "+27831234568",
      "UpdatedAt": "2026-05-28T09:10:00.000+02:00",
      "Appointments": []
    }
  ],
  "NextCursor": "MTIzNDU=",
  "HasMore": true
}

```

* An empty `Appointments` array is valid and indicates that the patient has consented and is actively on treatment, but all milestone dates have passed or have not yet been calculated.

---

## 5. Field Definitions

Patient Record (`Persons[]`) 

| Field | Type | Description |
| --- | --- | --- |
| `PersonId` | UUID (string) | Stable, permanent identifier for this patient. |
| `PhoneNumber` | string | Cellphone number in E.164 format (e.g. +27721234567). Omitted if not captured. |
| `UpdatedAt` | ISO-8601 datetime | Timestamp of the most recent change to this record. See Delta Sync section. |
| `Appointments` | array | Scheduled milestone appointments. May be empty. |

Appointment (`Appointments[]`) 

| Field | Type | Description |
| --- | --- | --- |
| `AppointmentDate` | date string (`yyyy-MM-dd`) | The scheduled appointment date. |
| `Facility.FacilityName` | string | DHIS facility name. |
| `Facility.Latitude` | number | WGS84 decimal degrees. Omitted if not captured for this facility. |
| `Facility.Longitude` | number | WGS84 decimal degrees. Omitted if not captured for this facility. |

Pagination Fields 

| Field | Type | Description |
| --- | --- | --- |
| `HasMore` | boolean | <br>`true` if additional pages exist. |
| `NextCursor` | string | Opaque cursor for the next page. Omitted when `HasMore` is false. |

---

6. Date and Timestamp Formats 

| Field | Format | Timezone | Example |
| --- | --- | --- | --- |
| `updatedSince` query parameter | ISO-8601 with timezone offset | Any - converted server-side | <br>`2026-06-01T00:00:00+02:00` or `2026-06-01T00:00:00Z` |
| `updatedAt` response field | ISO-8601 with timezone offset | SAST (UTC+2) | <br>`2026-05-30T14:22:00.000+02:00` |
| `appointmentDate` response field | `yyyy-MM-dd` (date only) | N/A | <br>`2026-06-20` |
| `ExpiresAt` / `RefreshExpiresAt` token fields | ISO-8601 with timezone offset | UTC (+00:00) | <br>`2026-06-02T08:00:00.000+00:00` |

* Token expiry timestamps are returned in UTC, while data timestamps (`updatedAt`) reflect South African Standard Time (SAST, UTC+2).

* The `updatedSince` parameter accepts any timezone-aware ISO-8601 value and is converted server-side.

---

## 7. Pagination Behaviour

* The API uses keyset (cursor-based) pagination.
* Page size is controlled by the `pageSize` parameter (default 100, maximum 500).

Recommended Sync Loop Pseudo-code 

```text
cursor = null
do:
    response = GET /persons/appointment-reminders?updatedSince=<lastSyncTime>&cursor=<cursor>&pageSize=500
    execute actions / upsert data
    cursor = response.NextCursor
while response.HasMore
```

* Cursor values are opaque; do not parse or store them beyond the current sync session.
* A new full sync (omitting `updatedSince` and `cursor`) can be performed at any time for onboarding or system recovery.

---

## 8. Delta Sync Behaviour

* Use the `updatedSince` parameter to retrieve only records that have changed since the last sync, minimizing routine polling payloads.
* A record's `updatedAt` value changes when:
  * The patient's demographics change (name, date of birth, cellphone number, etc.) 
  * The patient's WhatsApp consent status changes 
  * The patient's appointment calendar is recalculated (occurs when treatment data is updated or a new registration is saved) 
  
Recommended Delta Sync Strategy 
1. Record the maximum `UpdatedAt` value observed across all records returned in the current sync session.
2. On the next sync execution, pass this max value as the `updatedSince` parameter.
3. Upsert returned records locally by `PersonId` to overwrite the stored version.

**Handling Withdrawals:** If a patient's consent is withdrawn, they will no longer appear in subsequent delta responses. Remove or suppress their record locally upon the next full sync, or implement a periodic full sync routine for reconciliation.

---

## 9. Error Responses 

| HTTP Status | Meaning | Action |
| --- | --- | --- |
| **400 Bad Request** | Invalid query parameter (e.g. `pageSize` out of range, malformed `updatedSince`). | Fix the request configuration. Do not retry unchanged. |
| **401 Unauthorized** | Missing, expired, or invalid Bearer token. | Refresh the access token and retry once. If refresh fails, re-authenticate. |
| **403 Forbidden** | Authenticated but the account lacks the required role. | Contact WAMTech Systems; this indicates an account role configuration issue. |
| **500 Internal Server Error** | Unexpected server-side error. | Retry with exponential backoff. Log the `MessageDetail` field for support. |
| **503 Service Unavailable** | The gateway or upstream service is temporarily unavailable. | Retry with exponential backoff (identical to 500). Do not alert until 5 consecutive failures. |

### Retry Guidance 

| Error | Retry? | Strategy |
| --- | --- | --- |
| **400** | No | Fix request parameters. |
| **401** | Once | Refresh token, then retry the original request once. |
| **403** | No | Contact support. |
| **500** | Yes | Wait 30s, retry. If it fails again, double the wait (60s, 120s) up to a max of 10 minutes. Stop and alert after 5 consecutive failures. |
| **503** | Yes | Same exponential backoff as 500. Typically transient - do not alert unless sustained. |
| **Network timeout** | Yes | Same exponential backoff as 500. |

---

## 10. Rate Limits

* There are no hard rate limits currently enforced.
* **Constraint:** Do not poll more frequently than every 15 minutes. Prefer the sync frequencies below.
* If the server returns a `429 Too Many Requests` response in the future, treat it identically to a 500 error for retry behaviors and honor any provided `Retry-After` header.

---

## 11. Recommended Sync Frequency 

| Use Case | Recommended Frequency |
| --- | --- |
| **Routine appointment reminder delivery** | Every 4 hours using delta sync (`updatedSince`). |
| **Full reconciliation / recovery** | Once weekly full sync (omit `updatedSince`). |
| **Initial onboarding** | Full sync with `pageSize=500`. |

* Appointment dates are milestone dates computed from treatment start and do not change frequently.
* A 4-hour delta sync cadence is sufficient for timely reminder delivery without placing unnecessary load on the system.

---

## 12. Support and Escalation

* For integration issues, technical questions, or to request credential issuance, please reach out directly to WAMTech Systems.
