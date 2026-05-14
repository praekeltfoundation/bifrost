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

**Invite Sent**:
A historical state meaning the patient has already received the one-time invite and must not be invited again.
_Avoid_: currently eligible, active invite

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
- `invite_sent` could be read as current eligibility — resolved: **Invite Sent** is a one-way historical state
- "opted out" was used for delivery-triggered reminder disabling — resolved: use **Reminder Suppressed** unless the user explicitly withdrew consent
