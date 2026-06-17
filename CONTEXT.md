# Bifrost Synchronisation

Bifrost synchronises patient and appointment data from upstream clinical systems into local storage and Turn contact fields. Its core domain concern is deciding when synced clinical data is complete enough to drive patient messaging.

## Core Vocabulary

Shared terms used by both source-system syncs and Turn messaging updates.

**Turn Contact**:
The messaging-system contact identified by a single WhatsApp phone number and controlled through Turn contact fields.

**Facility**:
A CCMDD service location used in patient messaging; it may come from an appointment-bearing prescription or, when no future appointment exists, from the most recent prescription with a usable facility name. A usable **Facility** has a non-blank name; coordinates are optional.

**Reminder Suppressed**:
A messaging-side state meaning outbound reminders should not be sent to the patient until messaging is explicitly re-enabled by a human.
_Avoid_: opted out, blocked user, **Retired Messaging Contact**

**Consent Backfill**:
A one-off operation that updates one or more **Turn Contact** records to reflect clinic-captured reminder consent that already exists outside WhatsApp.

## SyNCH CCMDD

SyNCH CCMDD sync imports patients and prescriptions. Bifrost uses that data to update Turn contact fields, send one-time invites, and manage appointment reminders.

### Vocabulary

**Patient**:
A person synced from SyNCH CCMDD whose records may be imported into Turn.
_Avoid_: contact, user, EDRWeb Patient

**SyNCH Patient Identifier**:
The CCMDD patient identifier shared by SyNCH patient and prescription records for the same **Patient**.

**Complete Patient Delta**:
The patient changeset Bifrost expects from SyNCH for each sync window: every **Patient** whose own record changed, plus every **Patient** whose relevant prescription changed, even if the patient row itself did not.

**Relevant Prescription Filter**:
The upstream SyNCH rule that decides which patients appear on the limited patient endpoint based on prescription changes relevant to the integration; Bifrost depends on this filter for feed completeness but does not use it to decide messaging eligibility.
_Avoid_: local eligibility rule, appointment rule

**Upcoming Appointment**:
The earliest future-facing appointment date on or after today that belongs to a prescription with a resolvable **Facility** and can be used in patient messaging; ties on date are broken by the most recently created prescription.
_Avoid_: return date, next script date

**Messaging Phone Number**:
The most recent valid patient phone number available across that **Patient**'s prescriptions and used for all Turn messaging updates.
_Avoid_: latest prescription phone, raw patient phone

**New-Patient Eligible**:
A **Patient** who has a usable **Messaging Phone Number** and enough **Facility** context to send the one-time invite, even without an **Upcoming Appointment**.
_Avoid_: unsent invite, new patient

**Invite Sent**:
A historical state meaning the patient has already received the one-time invite and must not be invited again.
_Avoid_: current eligibility

**Messaging Contact Activation**:
The transition that makes a **Turn Contact** ready to receive welcome and reminder messaging for a **Patient**.

**Active Messaging Contact**:
The **Turn Contact** that Bifrost last configured for a **Patient**.

**Retired Messaging Contact**:
A former **Active Messaging Contact** that should no longer receive SyNCH reminders because the **Patient** moved to a different **Turn Contact**.
_Avoid_: **Reminder Suppressed**, blocked contact, opted-out contact

**Messaging Phone Number Change**:
A change where a **Patient**'s current **Messaging Phone Number** resolves to a different **Turn Contact** than the one previously used for the same **SyNCH Patient Identifier**.

### SyNCH Invariants

- A **Patient** may have many prescriptions and exactly one **SyNCH Patient Identifier**.
- A prescription references exactly one **SyNCH Patient Identifier**.
- A **Complete Patient Delta** may include a **Patient** whose own row did not change, and must include a **Patient** whose relevant prescription changed.
- A **Relevant Prescription Filter** may change upstream feed membership without changing Bifrost's local messaging rules.
- A **Patient** may have at most one current **Messaging Phone Number** and at most one **Active Messaging Contact**.
- A **Patient** without a successfully configured **Turn Contact** may have no **Active Messaging Contact**.
- Bifrost remembers a **Patient**'s **Active Messaging Contact** independently of current prescription data.
- **Active Messaging Contact** identity is compared using normalized **Messaging Phone Number** values.
- All Turn messaging updates for a **Patient** use the **Messaging Phone Number**.
- Turn facility fields may fall back globally to the most recent usable **Facility** when no usable **Upcoming Appointment** exists.
- An **Upcoming Appointment** belongs to exactly one prescription and must resolve to exactly one usable **Facility**.
- A **Patient** may become not **New-Patient Eligible** after being **Invite Sent**.
- An already **Invite Sent** **Patient** may be treated as having their current **Messaging Phone Number** as their **Active Messaging Contact** without new welcome messaging.

### SyNCH Contact Activation Invariants

A **Messaging Contact Activation**:

- establishes the **Patient**'s **Active Messaging Contact**
- covers first activation and activation after a **Messaging Phone Number Change**
- starts welcome messaging only after current patient and appointment context has been configured on the **Turn Contact**
- may rely on a prior context refresh rather than repeating patient and appointment context in the welcome trigger update
- may use a batch-level trigger timestamp shared across multiple **Patient** records

First **Messaging Contact Activation** happens before **Messaging Phone Number Change** handling in the same sync run, and may use the same welcome-trigger-only update shape as phone-change activation.

### SyNCH Phone-Number Change Invariants

A **Messaging Phone Number Change**:

- belongs to exactly one **Patient**
- applies only after the **Patient** already has an **Active Messaging Contact**
- requires a replacement **Messaging Phone Number**
- replaces the **Active Messaging Contact** with another **Turn Contact** for that **Patient**
- retires the previous **Active Messaging Contact** and makes it a **Retired Messaging Contact**
- requires **Messaging Contact Activation** for the new **Turn Contact**
- retires the previous **Active Messaging Contact** before welcome messaging is triggered on the new **Active Messaging Contact**
- depends on successful patient and appointment context refresh for the sync run
- succeeds only after messaging accepts both old-contact retirement and new-contact activation
- is retryable until both retirement and activation are accepted by the messaging system
- is tracked per **Patient**
- does not erase **Invite Sent**
- may require welcome messaging on the new **Active Messaging Contact** even when the **Patient** is already **Invite Sent**

Retiring a **Turn Contact** after a **Messaging Phone Number Change** disables reminders without clearing historical patient or appointment context fields, even when another **Patient** may share that **Turn Contact**. Welcome messaging on the new **Active Messaging Contact** is responsible for re-enabling reminders for that **Turn Contact**. A **Retired Messaging Contact** may become an **Active Messaging Contact** again if the **Patient**'s **Messaging Phone Number** changes back to it.

## EDRWeb

EDRWeb sync imports DR-TB appointment-reminder records. It is stored separately from SyNCH CCMDD and controls only EDRWeb reminder fields in Turn.

### Vocabulary

**EDRWeb Patient**:
A person synced from EDRWeb whose appointment reminder record may be imported into Turn.
_Avoid_: Patient, contact, user

**EDRWeb Patient Identifier**:
The stable EDRWeb identifier for an **EDRWeb Patient**.

**EDRWeb Appointment Reminder Feed**:
The EDRWeb source of consenting DR-TB patients and their milestone appointment dates for WhatsApp reminders.
_Avoid_: persons endpoint, appointment-reminders endpoint

**EDRWeb Appointment Reminder Delta Pull**:
A routine incremental pull of **EDRWeb Patient** records changed since the last completed appointment-reminder pull.

**EDRWeb Appointment Reminder Full Reconciliation Pull**:
A periodic full pull of the **EDRWeb Appointment Reminder Feed** that omits `updatedSince` so Bifrost can compare current feed membership with locally stored **EDRWeb Patient** snapshots.

**EDRWeb Appointment Reminder Feed Removal**:
A full-reconciliation finding where a locally stored **EDRWeb Patient** is absent from the current **EDRWeb Appointment Reminder Feed**, meaning Bifrost must record that the patient is no longer current for EDRWeb appointment reminder messaging.
_Avoid_: deletion, local data loss, Reminder Suppressed

**EDRWeb Appointment Reminder Checkpoint**:
The latest successfully stored EDRWeb `UpdatedAt` timestamp used to start the next **EDRWeb Appointment Reminder Delta Pull**.

**EDRWeb Appointment**:
A scheduled milestone appointment returned by the **EDRWeb Appointment Reminder Feed**.

**EDRWeb Reminder Eligible**:
An **EDRWeb Patient** who is current for EDRWeb appointment reminder messaging and has a usable WhatsApp phone number.
_Avoid_: user should receive messages, active user, opted in

**EDRWeb Messaging Contact Activation**:
The welcome-message transition that makes a **Turn Contact** ready to receive EDRWeb appointment reminder messaging for an **EDRWeb Patient**.
_Avoid_: opt in, Turn import step, contact setup

**EDRWeb Messaging Contact Activated**:
A historical state meaning EDRWeb welcome-message activation has already been accepted for the current **EDRWeb Active Messaging Contact**.
_Avoid_: new patient, new user, invite sent

**EDRWeb Active Messaging Contact**:
The **Turn Contact** that Bifrost last activated for an **EDRWeb Patient**.

**EDRWeb Messaging Phone Number Change**:
A change where an **EDRWeb Patient**'s usable phone number resolves to a different **Turn Contact** than the one previously activated for EDRWeb messaging.

### EDRWeb Sync Invariants

- An **EDRWeb Patient** has exactly one **EDRWeb Patient Identifier**.
- An **EDRWeb Patient** is stored separately from a SyNCH CCMDD **Patient** and has no shared patient identifier with one.
- The **EDRWeb Appointment Reminder Feed** may contain many **EDRWeb Patient** records.
- An **EDRWeb Patient** snapshot requires an **EDRWeb Patient Identifier** and an EDRWeb update timestamp.
- An **EDRWeb Appointment Reminder Delta Pull** stores one current local snapshot per **EDRWeb Patient**.
- An **EDRWeb Patient** stores current **EDRWeb Appointment** records as part of that local snapshot.
- **EDRWeb Appointment Reminder Delta Pull** queries from just before the **EDRWeb Appointment Reminder Checkpoint** to avoid missing same-timestamp EDRWeb updates.
- **EDRWeb Appointment Reminder Delta Pull** has no **EDRWeb Appointment Reminder Checkpoint** before any **EDRWeb Patient** snapshot exists locally.
- **EDRWeb Appointment Reminder Checkpoint** advances only after a successful **EDRWeb Appointment Reminder Delta Pull**.
- **EDRWeb Appointment Reminder Full Reconciliation Pull** omits `updatedSince` and compares current **EDRWeb Appointment Reminder Feed** membership with stored **EDRWeb Patient** snapshots.
- The **EDRWeb Appointment Reminder Feed** returns only upcoming **EDRWeb Appointment** records for reminder sync.
- An **EDRWeb Patient** may have no current **EDRWeb Appointment** records or captured WhatsApp phone number and still belong to the **EDRWeb Appointment Reminder Feed**.
- EDRWeb Turn contact updates use local **EDRWeb Patient** snapshots after EDRWeb pull changes have been committed.
- EDRWeb Turn contact update failures do not roll back completed **EDRWeb Appointment Reminder Delta Pull** or **EDRWeb Appointment Reminder Full Reconciliation Pull** snapshot changes.
- EDRWeb Turn contact updates refresh all locally stored **EDRWeb Patient** snapshots rather than only rows changed by the latest pull.

### EDRWeb Feed-Removal Invariants

**EDRWeb Appointment Reminder Feed Removal**:

- is detected only by a completed **EDRWeb Appointment Reminder Full Reconciliation Pull**
- does not prove why the **EDRWeb Patient** left the feed
- records that the **EDRWeb Patient** is no longer current for EDRWeb appointment reminder messaging
- leaves the stored **EDRWeb Patient** snapshot locally available
- is reversed by a later **EDRWeb Appointment Reminder Feed** record for the same **EDRWeb Patient Identifier**
- is later expressed to Turn through EDRWeb appointment reminder messaging fields only
- must not disable SyNCH reminder messaging for a shared **Turn Contact**

Expressing that an **EDRWeb Patient** should no longer receive EDRWeb reminders disables EDRWeb reminders without clearing historical EDRWeb patient or appointment context fields.

### EDRWeb Eligibility Invariants

- A usable WhatsApp phone number for an **EDRWeb Patient** is normalized the same way as a **Messaging Phone Number**.
- **EDRWeb Reminder Eligible** does not require appointment facility context.
- An **EDRWeb Reminder Eligible** **EDRWeb Patient** may have no current **EDRWeb Appointment** records.
- An **EDRWeb Reminder Eligible** **EDRWeb Patient** with multiple current **EDRWeb Appointment** records uses the earliest appointment date for Turn reminder context.
- Turn facility fields for EDRWeb reminder context come from the selected **EDRWeb Appointment**, and may be blank even when the appointment date is present.
- Updating Turn appointment reminder context for an **EDRWeb Reminder Eligible** **EDRWeb Patient** does not directly enable EDRWeb reminders and does not trigger **EDRWeb Messaging Contact Activation**.

### EDRWeb Contact Activation Invariants

**EDRWeb Messaging Contact Activation**:

- enables EDRWeb reminders after current EDRWeb patient and appointment context has been configured on the **Turn Contact**
- happens only after current EDRWeb patient and appointment context has been accepted by messaging
- is triggered by setting `edrweb_new_user` to an activation timestamp, without directly setting `edrweb_reminders` to true

**EDRWeb Messaging Contact Activated** is recorded only after messaging accepts **EDRWeb Messaging Contact Activation**. Failed EDRWeb retirement or activation rows remain retryable without blocking successfully accepted EDRWeb activation rows.

### EDRWeb Phone-Number Change Invariants

- An **EDRWeb Patient** may have at most one **EDRWeb Active Messaging Contact**.
- **EDRWeb Active Messaging Contact** identity is compared using normalized WhatsApp phone number values.
- An **EDRWeb Messaging Phone Number Change** replaces the **EDRWeb Active Messaging Contact**.
- An **EDRWeb Messaging Phone Number Change** retires the previous **EDRWeb Active Messaging Contact** before triggering **EDRWeb Messaging Contact Activation** for the new **Turn Contact**.
- An **EDRWeb Messaging Phone Number Change** requires both old-contact retirement and new-contact activation to be accepted before Bifrost updates the stored **EDRWeb Active Messaging Contact**.

## OTP Delivery

OTP delivery is phone-number based. It can run before Bifrost can map a recipient to stored patient state.

**OTP Delivery Request**:
A request from SyNCH to send a one-time-passcode WhatsApp template message to exactly one **Turn Contact** identified only by phone number. It does not require a **Patient**.

**OTP Recipient Type**:
The business-role classification attached to an **OTP Delivery Request** that tells SyNCH and Bifrost whether the recipient is the **Patient** or a SyNCH user, without changing delivery behavior in the current version. The public API uses the values `patient` and `synch_user`.
_Avoid_: template selector, auth workflow, contact type

**API Caller**:
The authenticated Django user account whose token authorises an inbound **OTP Delivery Request**.
_Avoid_: patient, recipient, Turn Contact

**Delivery Protection**:
Messaging-side safeguards that may reject or slow **OTP Delivery Requests** to protect channel health, independent of OTP validity policy.
_Avoid_: OTP expiry, resend policy, verification rules

**Provider Message ID**:
The messaging-provider identifier returned when an **OTP Delivery Request** is accepted for downstream delivery.

**Unknown Delivery Outcome**:
A temporary upstream failure state where Bifrost cannot tell whether the messaging provider accepted an **OTP Delivery Request**.
_Avoid_: guaranteed failure, duplicate-proof retry

### OTP Invariants

- An **OTP Delivery Request** must declare exactly one **OTP Recipient Type** and is authorised by exactly one **API Caller**.
- **OTP Recipient Type** may be `patient` even when Bifrost cannot map the request to a stored **Patient**.
- **OTP Recipient Type** is request-scoped and does not permanently classify a **Turn Contact**.
- **Delivery Protection** may reject an **OTP Delivery Request** even when the caller's OTP policy would otherwise allow it.
- **Delivery Protection** for OTP sending is separate from **Reminder Suppressed**.
- A successful **OTP Delivery Request** may produce one **Provider Message ID**.
- A temporary upstream failure may leave an **OTP Delivery Request** in **Unknown Delivery Outcome**.

## Suppression And Delivery Failure

Suppression is messaging state. Delivery-failure provenance records why suppression may have been applied.

**Suppression Origin Message**:
The first outbound message ID whose permanent delivery failure caused a **Turn Contact** to become **Reminder Suppressed**.

**Delivery-Failure Provenance**:
The stored evidence that a permanent delivery failure contributed to reminder suppression for a **Turn Contact**.

### Suppression Invariants

- **Retired Messaging Contact** is separate from **Reminder Suppressed**.
- A **Turn Contact** may become **Reminder Suppressed** for reasons other than delivery failure.
- **Delivery-Failure Provenance** is distinct from the general **Reminder Suppressed** state.
- A **Reminder Suppressed** **Turn Contact** keeps its original **Suppression Origin Message** even if later permanent delivery failures occur.
- A permanent delivery failure on a shared WhatsApp line is treated as evidence that SynCH reminders to the same **Turn Contact** will also fail, even if the failed message came from another service.

## Cross-System Rules

Cross-system rules describe shared Turn contacts and boundaries between SyNCH and EDRWeb messaging.

- A **Turn Contact** corresponds to exactly one WhatsApp phone number.
- A **Turn Contact** may receive SyNCH and EDRWeb messaging fields when an **EDRWeb Patient** and a **Patient** share the same WhatsApp phone number.
- A **Turn Contact** may hold the **SyNCH Patient Identifier** for the **Patient** currently associated with its phone number.
- A **Turn Contact** can hold only one current **SyNCH Patient Identifier**, even when multiple **Patient** records share its **Messaging Phone Number**.
- A **Patient** may map to different **Turn Contact** records over time as their **Messaging Phone Number** changes.
- Multiple **Patient** records may map to the same **Turn Contact** when they share a **Messaging Phone Number**.
- Shared **Messaging Phone Number** conflict protection remains out of scope.
- **Consent Backfill** may set reminder-consent fields without changing whether a **Patient** is **Invite Sent**.
- **Consent Backfill** may use a CSV export as its source of facility text when production patient state is unavailable locally.

## Decision Notes

- Use source-specific patient language: **Patient** for SyNCH CCMDD; **EDRWeb Patient** for EDRWeb. Bifrost does not reconcile them because no shared upstream identifier exists.
- Treat `ccmdd_patient_id` and prescription `patient_id` as the same **SyNCH Patient Identifier**.
- Keep upstream feed rules separate from local messaging rules: **Relevant Prescription Filter** controls feed membership, not eligibility.
- Use the most recent valid **Messaging Phone Number**. Missing or invalid current phone data is not a **Messaging Phone Number Change**; a replacement **Messaging Phone Number** is required.
- Treat **Invite Sent** as patient-level history, not current eligibility. After a **Messaging Phone Number Change**, the new **Active Messaging Contact** may still receive welcome messaging.
- Let welcome messaging re-enable reminders after Bifrost sets the welcome trigger; Bifrost does not directly set reminder consent during activation.
- Use **Reminder Suppressed** only for reminder suppression; OTP **Delivery Protection**, **Retired Messaging Contact**, EDRWeb feed removal, and explicit consent withdrawal are separate concepts.
- Use **Consent Backfill** only for one-off mirroring of existing clinic consent into Turn.
- OTP delivery is request-scoped: **Turn Contact** recipient, **API Caller** principal, **OTP Recipient Type** business role, unchanged delivery behavior across recipient types.
- Prefer **Provider Message ID** over echoed phone number in successful OTP responses. Treat upstream timeout as **Unknown Delivery Outcome**.
- Use EDRWeb reminder fields only for **EDRWeb Appointment Reminder Feed Removal**; do not suppress SyNCH reminders.
- Setting EDRWeb appointment reminder context does not enable EDRWeb reminders; enabling happens through **EDRWeb Messaging Contact Activation**, and accepted state follows the **EDRWeb Active Messaging Contact**.

## Example Dialogue

> **Dev:** "This patient has a return date next week, so should we mark them as new?"
> **Domain expert:** "A future appointment still drives the appointment date, but invite eligibility is broader: if we have a **Messaging Phone Number** and enough facility context for the invite, they can still be **New-Patient Eligible** even without an **Upcoming Appointment**."
