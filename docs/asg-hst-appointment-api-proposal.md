# Provider API Requirements for Appointment Reminder Data

**Document status:** Draft for provider review  
**Audience:** External system/provider technical team  
**Purpose:** Define the minimum API capability required for Reach Digital Health to consume appointment data from the provider system and send WhatsApp appointment reminders to consenting users.

---

## 1. Summary

Reach Digital Health requires a secure read-only API that exposes appointment reminder data for users who have consented to receive WhatsApp messages.

The provider system must filter users before exposing data. Only users who Reach is permitted to message for appointment reminders should appear in the API response. Users who should not receive WhatsApp appointment reminders must not be returned by the API.

The integration is for appointment reminders only. It does not cover missed appointment workflows, attendance tracking, clinical record sharing, or other reminder types.

### 1.1 Minimal Endpoint Summary

At minimum, the provider should expose an endpoint equivalent to:

```http
GET /appointment-reminder-data?updated_since=<timestamp>
```

The endpoint should return:

- Patient unique identifier
- Phone number
- Record update timestamp
- Current appointment list for that patient
- Appointment date for each appointment
- Facility name for each appointment
- Optional facility coordinates
- Pagination cursor, if more data is available

This minimal API is sufficient for Reach to consume appointment data and send WhatsApp reminders for each user's next upcoming appointment without pulling the full dataset on every sync.

---

## 2. Objectives

The API must allow Reach to:

1. Authenticate securely over HTTPS.
2. Retrieve only users who have consented to receive WhatsApp appointment reminders.
3. Include a patient phone number to send reminders to.
4. Retrieve the list of upcoming appointment dates for that patient.
5. Retrieve minimum facility details for each appointment, specifically facility name.
6. Optionally retrieve facility coordinates.
7. Pull only records updated after a supplied timestamp.
8. Sync only changed records into Reach systems, using upsert logic to update existing records and create new ones without requiring a full dataset pull each time.
9. Send reminders for the next upcoming appointment only.

---

## 3. Scope

### 3.1 In Scope

The provider must expose an HTTPS API that supports read-only retrieval of appointment reminder data.

The API must support:

- Secure authentication
- Provider-side filtering so that only consenting and eligible users are exposed
- One result per unique patient ID
- One phone number per patient
- Retrieval of appointment dates
- Retrieval of facility name for each appointment
- Optional retrieval of facility coordinates
- Delta sync using an `updated_since` timestamp or equivalent
- Pagination, if the dataset size requires it
- A QA, staging, or sandbox environment for testing
- API documentation covering request formats, response formats, errors, and retry behaviour

### 3.2 Out of Scope

The following are out of scope:

- Reach creating, updating, cancelling, or deleting appointments in the provider system
- Reach writing any data back to the provider system
- Attendance tracking
- Missed appointment follow-up
- Clinical record retrieval
- Medication pickup reminders
- Lab result notifications
- Patient-facing appointment lookup
- Alternate or backup phone numbers
- Shared phone numbers across multiple patients
- Any reminder type other than appointment reminders

---

## 4. User Eligibility and Consent

Only users who have consented to receive WhatsApp appointment reminders must appear in the API.

The provider system is responsible for applying this filtering before data is exposed. Reach should not receive users who are not eligible or who should not be messaged.

The API must exclude:

- Users who have not consented to WhatsApp appointment reminders
- Users who have withdrawn consent
- Users who the provider does not want Reach to message

Reach assumes that any user returned by the API is eligible to receive appointment reminders over WhatsApp.

---

## 5. Identifier and Phone Number Rules

### 5.1 Patient ID as Unique Identifier

For this integration, a unique identifier should be supplied to be used by Reach.

The API must return exactly one result per patient identifier.

### 5.2 Phone Number Ownership

The provider must guarantee that the phone number belongs to the patient before the record is exposed through the API.

The API must not return phone numbers belonging to caregivers, family members, nominated collectors, facility staff, or other third parties.

### 5.3 Phone Number Format

Phone numbers should preferably be returned in international E.164 format.

Example:

```text
+27821234567
```

If another format is used, the provider must document the format and ensure it is consistent.

### 5.4 Phone Number Changes

If a patient changes phone number, the new number should be visible on the API, along with an updated timestamp, so that the change can be synced.

---

## 6. Appointment Reminder Behaviour

Reach will send reminders for the next upcoming appointment only.

If a patient has multiple future appointments, Reach will:

1. Order appointments by date.
2. Select the earliest upcoming appointment.
3. Send reminders for that appointment.
4. Once that appointment date has passed, select the next upcoming appointment from the list.

The provider API should therefore return the current list of appointment dates for the patient, ordered by date if possible.

Reach does not require appointment time, appointment type, appointment notes, attendance status, or missed appointment status.

---

## 7. Appointment Data Requirements

For each appointment, the API must provide:

- Appointment date
- Facility details for the facility where the appointment will take place

The API does not need to provide:

- Appointment time
- Appointment type
- Appointment notes
- Attendance status
- Missed appointment status
- Clinical details

### 7.1 Appointment Date

Appointment dates must be returned in ISO-8601 date format.

Example:

```text
2026-06-20
```

If the provider system stores appointment times, they do not need to be exposed for this integration.

### 7.2 Cancelled, Deleted, or Rescheduled Appointments

If appointments are cancelled, deleted, rescheduled, or otherwise changed, the list of appointments returned by the API must reflect the latest current state on the next API fetch, and the patient `updated_at` timestamp should be updated.

Reach will treat the returned appointment list as the current source of truth for that patient.

This means:

- Cancelled appointments should be removed from the returned appointment list
- Deleted appointments should be removed from the returned appointment list
- Rescheduled appointments should appear with the new appointment date
- Changed records must reappear in delta sync results
- Reach will upsert the full appointment list for the patient ID

The provider does not need to return detailed cancellation or deletion reasons.

---

## 8. Facility Data Requirements

For each appointment, the API must provide the facility name.

Facility coordinates are optional but useful.

No other facility fields are required.

| Field | Required | Description |
|---|---:|---|
| `facility_name` | Yes | Name of the clinic/facility where the appointment will take place. |
| `latitude` | Optional | Facility latitude. |
| `longitude` | Optional | Facility longitude. |

The API does not need to provide facility address, phone number, email address, department, room, or service point unless separately agreed.

---

## 9. Sync Model

### 9.1 Scheduled Pull

Reach will poll the provider API on a scheduled basis.

The provider may specify how often Reach should pull from the API. The recommended sync frequency should be included in the API documentation.

### 9.2 Delta Sync

The API must support pulling records updated after a supplied timestamp.

The request should support a parameter equivalent to:

```text
updated_since=<timestamp>
```

Example request:

```http
GET /appointment-reminder-data?updated_since=2026-05-20T00:00:00Z&...
Authorization: Bearer <token>
```

The exact endpoint path and parameter names may differ, but the API must provide equivalent behaviour.

### 9.3 Records Included in Delta Sync

When Reach supplies an `updated_since` timestamp, the API must return records where either of the following has changed after that timestamp:

- The user record relevant to appointment reminders
- The phone number record
- The list of appointments for the patient
- The facility details required for a returned appointment

This includes cases where appointments have been cancelled or deleted and therefore removed from the current appointment list.

### 9.4 Patient Records Are Not Removed

Patient records should not be removed from the API as a way of communicating changes.

Instead:

- New patient records may be added
- Existing records may be modified
- Appointment lists may be updated
- Appointment lists may become empty

Reach will upsert changed records on its side.

### 9.5 Upsert Behaviour

Reach will treat each returned patient record as the latest state for that patient.

On each sync, Reach will upsert the record by patient identifier and replace the stored appointment list with the appointment list returned by the provider.

This requires the provider to return the complete current appointment list for the patient whenever that patient appears in a delta sync response.

### 9.6 Full Sync

The provider should support a full sync for initial onboarding, recovery, and reconciliation.

A full sync should return all currently eligible users and their current appointment lists.

### 9.7 Pagination

If the dataset is large, the API must support pagination.

The response should include:

- A list of records
- A `next_page_token`, cursor, or equivalent value when more records are available
- A clear indication when the final page has been reached

Cursor-based pagination is preferred.

---

## 10. Authentication and Transport Security

### 10.1 HTTPS

All API traffic must be over HTTPS.

Plain HTTP must not be used for production traffic.

### 10.2 Authentication

Any standard authentication mechanism is acceptable, provided it is secure and documented.

Examples include:

- Bearer token authentication
- Token-based authentication
- Digest authentication

The selected mechanism must be documented in the API specification.

### 10.3 Credential Delivery

Credentials must be supplied to Reach through a secure and encrypted channel.

Credentials must not be sent over plain email, chat, or other insecure channels.

### 10.4 Permission Assumption

It is assumed that Reach has permission to receive and use the exposed data for the purpose of sending appointment reminders.

The provider is responsible for ensuring that only users who may be messaged are exposed by the API.

---

## 11. Required Data Model

The exact field names may differ, but the API must provide equivalent data.

### 11.1 Patient Record

| Field | Required | Description |
|---|---:|---|
| `patient_id` | Yes | Unique identifier for the patient. |
| `phone_number` | Yes | Patient phone number used as the identifier by Reach. Prefer E.164 format. |
| `updated_at` | Yes | Timestamp representing the latest update to the user, phone number, appointment list, or relevant facility details. |
| `appointments` | Yes | Current list of future appointments for this patient. May be empty. |

### 11.2 Appointment Record

| Field | Required | Description |
|---|---:|---|
| `appointment_date` | Yes | Appointment date in ISO-8601 date format. |
| `facility` | Yes | Facility details for the appointment. |

### 11.3 Facility Record

| Field | Required | Description |
|---|---:|---|
| `facility_name` | Yes | Clinic/facility name. |
| `latitude` | Optional | Facility latitude. |
| `longitude` | Optional | Facility longitude. |

---

## 12. Proposed Response Shape

The provider may propose a different response format, but the API should support a structure equivalent to the following.

```json
{
  "next_page_token": "eyJwYWdlIjoyfQ==",
  "data": [
    {
      "patient_id": "abc123",
      "phone_number": "+27821234567",
      "updated_at": "2026-05-19T15:30:00Z",
      "appointments": [
        {
          "appointment_date": "2026-06-20",
          "facility": {
            "facility_name": "Example Clinic",
            "latitude": -29.8587,
            "longitude": 31.0218
          }
        },
        {
          "appointment_date": "2026-07-18",
          "facility": {
            "facility_name": "Example Clinic",
            "latitude": -29.8587,
            "longitude": 31.0218
          }
        }
      ]
    },
    {
      "patient_id": "def456",
      "phone_number": "+27820000001",
      "updated_at": "2026-05-19T16:10:00Z",
      "appointments": []
    }
  ]
}
```

The second record demonstrates how a patient can remain present with an empty appointment list, for example after appointments have been removed or cancelled.

---

## 13. Error Handling and Retry Behaviour

The API documentation must describe all expected error responses and formats.

The documentation must also describe the allowed retry strategy for each error type.

At minimum, documentation should cover:

- Authentication failures
- Invalid request parameters
- Invalid `updated_since` timestamp
- Rate limiting
- Temporary provider-side failures
- Planned downtime or maintenance
- Pagination errors

The API should use standard HTTP status codes where possible.

| Status | Meaning | Retry Expectation |
|---:|---|---|
| 200 | Request succeeded. | Not applicable. |
| 400 | Invalid request parameter. | Do not retry without changing the request. |
| 401 | Missing or invalid authentication. | Do not retry until credentials are fixed. |
| 403 | Authenticated but not authorised. | Do not retry until permissions are fixed. |
| 429 | Rate limit exceeded. | Retry according to provider guidance, preferably using `Retry-After`. |
| 500 | Unexpected provider-side error. | Retry according to provider guidance. |
| 503 | Service temporarily unavailable. | Retry according to provider guidance. |

Example error response:

```json
{
  "error": {
    "code": "INVALID_UPDATED_SINCE",
    "message": "updated_since must be an ISO-8601 timestamp with timezone"
  }
}
```

---

## 14. Environments

The provider must make a QA, staging, or sandbox environment available.

Reach must be able to connect this non-production provider environment to Reach's QA environment for integration testing.

The QA/staging/sandbox environment should behave like production as closely as possible for the supported API features.

---

## 15. Test Data Requirements

The QA/staging/sandbox environment must contain test data that allows Reach to validate the integration.

First prize is for the provider to allow Reach to create and manage test data in the provider system so that Reach QA can conduct full end-to-end testing.

At minimum, test data should support the following scenarios:

1. A consenting patient with one future appointment.
2. A consenting patient with multiple future appointments.
3. Appointment list ordered by date.
4. Appointment removed from the list because it was cancelled or deleted.
5. Appointment date changed because it was rescheduled.
6. Patient phone number changed.
7. A patient with no current future appointments, represented by an empty appointment list.
8. Facility name present for every appointment.
9. Facility coordinates present where available.
10. Delta sync returning records changed after a supplied timestamp.

---

## 16. API Documentation Requirements

The provider must supply API documentation that includes:

- Base URLs for QA/staging/sandbox and production
- Authentication mechanism
- Secure credential exchange process
- Endpoint paths
- Query parameters
- Request examples
- Response examples
- Field definitions
- Date and timestamp formats
- Pagination behaviour
- Delta sync behaviour
- Error response formats
- Retry guidance per error type
- Rate limits, if any
- Recommended sync frequency
- Support or escalation process for integration issues

An OpenAPI/Swagger specification is preferred but not mandatory.

---

## 17. Acceptance Criteria

The integration will be considered ready for use when the provider has demonstrated that:

1. Reach can authenticate successfully over HTTPS.
2. Credentials have been supplied through a secure and encrypted channel.
3. The API returns only users who may receive WhatsApp appointment reminders.
4. The API returns exactly one record per patient identifier.
5. Each returned phone number is guaranteed to belong to the patient.
6. Each record includes a current appointment list, even if empty.
7. Each appointment includes an appointment date.
8. Each appointment includes a facility name.
9. Facility coordinates are returned where available, if supported.
10. Cancelled, deleted, or rescheduled appointments are reflected in the next API fetch.
11. The API supports pulling records updated after a supplied timestamp.
12. Changed records reappear in delta sync results.
13. Reach can upsert returned records by patient identifier.
14. The provider has documented recommended sync frequency.
15. The provider has documented error responses and retry strategies.
16. A QA/staging/sandbox environment is available.
17. Test data is available for Reach QA to test the integration end to end.

---

## 18. Open Items for Provider Confirmation

The following items should be confirmed by the provider before implementation:

| Item | Provider to Confirm |
|---|---|
| Authentication mechanism | Confirm selected mechanism and credential exchange process. |
| Phone number format | Confirm whether E.164 format will be used. |
| Delta timestamp semantics | Confirm which updates cause `updated_at` to change. |
| Full sync support | Confirm whether full sync is available for onboarding and recovery. |
| Pagination | Confirm whether pagination is needed and how it works. |
| Sync frequency | Confirm recommended polling frequency. |
| Facility coordinates | Confirm whether latitude/longitude can be supplied. |
| QA/staging/sandbox environment | Confirm environment URL and access process. |
| Test data creation | Confirm whether Reach can create test data for end-to-end QA. |
| Error and retry documentation | Confirm final documented error formats and retry guidance. |

