# Bifrost Synchronisation

Bifrost synchronises patient and appointment data from SyNCH CCMDD into local storage and Turn contact fields. Its core domain concern is deciding when synced clinical data is complete enough to drive patient messaging.

## Language

**Patient**:
A person synced from SyNCH CCMDD whose records may be imported into Turn.
_Avoid_: contact, user, member

**Facility**:
A CCMDD service location used in patient messaging; it may come from an appointment-bearing prescription or, when no future appointment exists, from the most recent prescription with a usable facility name.
_Avoid_: clinic data, site payload

**Upcoming Appointment**:
The earliest future-facing appointment date on or after today that belongs to a prescription with a resolvable **Facility** and can be used in patient messaging; ties on date are broken by the most recently created prescription.
_Avoid_: return date, next script date

**New-Patient Eligible**:
A **Patient** who has a usable phone number and enough facility context to send the one-time invite, even if they do not currently have an **Upcoming Appointment**.
_Avoid_: unsent invite, new patient

**Messaging Phone Number**:
The most recent valid patient phone number available across that **Patient**'s prescriptions and used for all Turn messaging updates.
_Avoid_: latest prescription phone, raw patient phone

**Turn Contact**:
The messaging-system contact identified by a single WhatsApp phone number and controlled through Turn contact fields.
_Avoid_: patient, person

**OTP Delivery Request**:
A request from SyNCH to send a one-time-passcode WhatsApp template message to a **Turn Contact** identified only by phone number.
_Avoid_: patient verification, user signup, OTP record

**API Caller**:
The authenticated Django user account whose token authorises an inbound **OTP Delivery Request**.
_Avoid_: patient, recipient, Turn Contact

**Delivery Protection**:
Messaging-side safeguards that may reject or slow **OTP Delivery Requests** to protect channel health, independent of OTP validity policy.
_Avoid_: OTP expiry, resend policy, verification rules

**Provider Message ID**:
The messaging-provider identifier returned when an **OTP Delivery Request** is accepted for downstream delivery.
_Avoid_: patient id, OTP id, verification id

**Unknown Delivery Outcome**:
A temporary upstream failure state where Bifrost cannot tell whether the messaging provider accepted an **OTP Delivery Request**.
_Avoid_: guaranteed failure, duplicate-proof retry

**Invite Sent**:
A historical state meaning the patient has already received the one-time invite and must not be invited again.
_Avoid_: currently eligible, active invite

**Consent Backfill**:
A one-off operation that updates a **Turn Contact** to reflect clinic-captured reminder consent that already exists outside WhatsApp.
_Avoid_: unsuppress script, opt-in migration, reminder reset

**Reminder Suppressed**:
A messaging-side state meaning outbound reminders should not be sent to the patient until messaging is explicitly re-enabled by a human.
_Avoid_: opted out, blocked user

**Suppression Origin Message**:
The first outbound message ID whose permanent delivery failure caused a **Turn Contact** to become **Reminder Suppressed**.
_Avoid_: latest failure message, last blocked message

**Delivery-Failure Provenance**:
The stored evidence that a permanent delivery failure contributed to reminder suppression for a **Turn Contact**.
_Avoid_: suppression reason field, latest delivery error

## Relationships

- A **Patient** may have many prescriptions
- A **Patient** may have at most one current **Messaging Phone Number**
- A **Patient** may map to different **Turn Contact** records over time as their **Messaging Phone Number** changes
- A **Patient** may be **New-Patient Eligible** even when they have no **Upcoming Appointment**
- A **Patient** is **New-Patient Eligible** only when they have a **Messaging Phone Number**
- A **Patient** is **New-Patient Eligible** only when they also have a usable **Facility**
- A **Patient** may become not **New-Patient Eligible** after being **Invite Sent**
- An **Upcoming Appointment** belongs to exactly one prescription
- An **Upcoming Appointment** must resolve to exactly one usable **Facility**
- A **Turn Contact** corresponds to exactly one WhatsApp phone number
- A **Consent Backfill** targets one or more **Turn Contact** records
- A **Consent Backfill** may set reminder-consent fields without changing whether a **Patient** is **Invite Sent**
- A **Consent Backfill** may use a CSV export as its source of facility text when production patient state is unavailable locally
- An **OTP Delivery Request** targets exactly one **Turn Contact**
- An **OTP Delivery Request** does not require a **Patient**
- An **OTP Delivery Request** is authorised by exactly one **API Caller**
- **Delivery Protection** may reject an **OTP Delivery Request** even when the caller's OTP policy would otherwise allow it
- **Delivery Protection** for OTP sending is separate from **Reminder Suppressed**
- A successful **OTP Delivery Request** may produce one **Provider Message ID**
- A temporary upstream failure may leave an **OTP Delivery Request** in **Unknown Delivery Outcome**
- A **Turn Contact** may become **Reminder Suppressed** for reasons other than delivery failure
- **Delivery-Failure Provenance** is distinct from the general **Reminder Suppressed** state
- A **Reminder Suppressed** **Turn Contact** keeps its original **Suppression Origin Message** even if later permanent delivery failures occur
- A permanent delivery failure on a shared WhatsApp line is treated as evidence that SynCH reminders to the same **Turn Contact** will also fail, even if the failed message came from another service
- All Turn messaging updates for a **Patient** use the **Messaging Phone Number**
- Turn facility fields may fall back globally to the most recent usable **Facility** whenever no usable **Upcoming Appointment** exists
- A usable **Facility** must have a non-blank name; coordinates are optional

## Example dialogue

> **Dev:** "This patient has a return date next week, so should we mark them as new?"
> **Domain expert:** "A future appointment still drives the appointment date, but invite eligibility is broader: if we have a **Messaging Phone Number** and enough facility context for the invite, they can still be **New-Patient Eligible** even without an **Upcoming Appointment**."

## Flagged ambiguities

- "new patient" was used to mean both a newly synced **Patient** and a **New-Patient Eligible** patient — resolved: messaging should use **New-Patient Eligible**
- "latest phone number" was used to mean the newest stored phone value, even when unusable — resolved: messaging should use the most recent valid **Messaging Phone Number**
- "user" was used for pre-sync OTP recipients — resolved: use **Turn Contact** for the recipient and **OTP Delivery Request** for the API call
- "user" was used for inbound authentication — resolved: use **API Caller** for the authenticated Django principal
- `429` could be read as OTP resend policy — resolved: use **Delivery Protection** for Bifrost-side channel safeguards and keep OTP lifecycle ownership in SyNCH
- reminder suppression could be read as a general messaging block — resolved: OTP **Delivery Protection** and **Reminder Suppressed** are separate mechanisms
- "opt in" was used to mean both clinic-captured consent and a WhatsApp button press — resolved: use **Consent Backfill** for the one-off operation that mirrors existing clinic consent into Turn
- Turn facility fields are normally derived from synced data, but this one-off reminder consent backfill may source the template facility name from the exported Turn CSV when local production sync data is unavailable
- echoed `msisdn` could be treated as the response identifier — resolved: prefer **Provider Message ID** over returning phone number in success responses
- upstream timeout could be read as a definite non-send — resolved: treat it as **Unknown Delivery Outcome**
- `invite_sent` could be read as current eligibility — resolved: **Invite Sent** is a one-way historical state
- "opted out" was used for delivery-triggered reminder disabling — resolved: use **Reminder Suppressed** unless the user explicitly withdrew consent
