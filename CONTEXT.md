# Bifrost Synchronisation

Bifrost synchronises patient and appointment data from upstream clinical systems into local storage and Turn contact fields. Its core domain concern is deciding when synced clinical data is complete enough to drive patient messaging.

## Language

**Patient**:
A person synced from SyNCH CCMDD whose records may be imported into Turn.
_Avoid_: contact, user, member

**EDRWeb Patient**:
A person synced from EDRWeb whose appointment reminder record may be imported into Turn.
_Avoid_: Patient, EDRWeb person, contact, user, member

**SyNCH Patient Identifier**:
The CCMDD patient identifier shared by SyNCH patient and prescription records for the same **Patient**.
_Avoid_: prescription patient id, local patient id

**EDRWeb Patient Identifier**:
The stable EDRWeb identifier for an **EDRWeb Patient**.
_Avoid_: person id, local patient id, SyNCH patient id

**EDRWeb Appointment Reminder Feed**:
The EDRWeb source of consenting DR-TB patients and their milestone appointment dates for WhatsApp reminders.
_Avoid_: persons endpoint, appointment-reminders endpoint, EDRWeb sync

**EDRWeb Appointment Reminder Delta Pull**:
A routine incremental pull of **EDRWeb Patient** records changed since the last completed appointment-reminder pull.
_Avoid_: frequent fetch, updated data fetch, EDRWeb sync

**EDRWeb Appointment Reminder Checkpoint**:
The latest successfully stored EDRWeb `UpdatedAt` timestamp used to start the next **EDRWeb Appointment Reminder Delta Pull**.
_Avoid_: cursor, last run time, latest local update

**EDRWeb Appointment**:
A scheduled milestone appointment returned by the **EDRWeb Appointment Reminder Feed**.
_Avoid_: SyNCH return date, prescription appointment, local appointment

**Complete Patient Delta**:
The patient changeset Bifrost expects from SyNCH for each sync window: every **Patient** whose own record changed, plus every **Patient** whose relevant prescription changed, even if the patient row itself did not.
_Avoid_: full patient snapshot, patient-only delta

**Relevant Prescription Filter**:
The upstream SyNCH rule that decides which patients appear on the limited patient endpoint based on prescription changes relevant to the integration; Bifrost depends on this filter for feed completeness but does not use it to decide messaging eligibility.
_Avoid_: local eligibility rule, appointment rule

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

**Messaging Phone Number Change**:
A change where a **Patient**'s current **Messaging Phone Number** resolves to a different **Turn Contact** than the one previously used for the same **SyNCH Patient Identifier**.
_Avoid_: user phone change, contact change, new phone prescription

**Messaging Contact Activation**:
The transition that makes a **Turn Contact** ready to receive welcome and reminder messaging for a **Patient**.
_Avoid_: Turn import step, contact setup

**Active Messaging Contact**:
The **Turn Contact** that Bifrost last configured for a **Patient**.
_Avoid_: old phone, current user contact, latest Turn contact

**Retired Messaging Contact**:
A former **Active Messaging Contact** that should no longer receive SyNCH reminders because the **Patient** moved to a different **Turn Contact**.
_Avoid_: suppressed contact, blocked contact, opted-out contact

**Turn Contact**:
The messaging-system contact identified by a single WhatsApp phone number and controlled through Turn contact fields.
_Avoid_: patient, person

**OTP Delivery Request**:
A request from SyNCH to send a one-time-passcode WhatsApp template message to a **Turn Contact** identified only by phone number.
_Avoid_: patient verification, user signup, OTP record

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
- A **Patient** has exactly one **SyNCH Patient Identifier**
- An **EDRWeb Patient** has exactly one **EDRWeb Patient Identifier**
- An **EDRWeb Patient** is stored separately from a SyNCH CCMDD **Patient**
- An **EDRWeb Patient** has no shared patient identifier with a SyNCH CCMDD **Patient**
- The **EDRWeb Appointment Reminder Feed** may contain many **EDRWeb Patient** records
- An **EDRWeb Patient** snapshot requires an **EDRWeb Patient Identifier** and an EDRWeb update timestamp
- An **EDRWeb Appointment Reminder Delta Pull** stores one current local snapshot per **EDRWeb Patient**
- An **EDRWeb Patient** stores its current **EDRWeb Appointment** records as part of that local snapshot
- An **EDRWeb Patient** may have no current **EDRWeb Appointment** records and still belong to the **EDRWeb Appointment Reminder Feed**
- An **EDRWeb Patient** may have no captured WhatsApp phone number and still belong to the **EDRWeb Appointment Reminder Feed**
- An **EDRWeb Appointment Reminder Delta Pull** queries from just before the **EDRWeb Appointment Reminder Checkpoint** to avoid missing same-timestamp EDRWeb updates
- An **EDRWeb Appointment Reminder Delta Pull** has no **EDRWeb Appointment Reminder Checkpoint** before any **EDRWeb Patient** snapshot exists locally
- An **EDRWeb Appointment Reminder Checkpoint** advances only after a successful **EDRWeb Appointment Reminder Delta Pull**
- The **EDRWeb Appointment Reminder Feed** returns only upcoming **EDRWeb Appointment** records for reminder sync
- A prescription references exactly one **SyNCH Patient Identifier**
- A **Complete Patient Delta** may include a **Patient** whose own record did not change
- A **Complete Patient Delta** must include a **Patient** whose relevant prescription changed
- A **Relevant Prescription Filter** may change which **Patient** records appear in the upstream patient feed without changing Bifrost's own messaging rules
- A **Patient** may have at most one current **Messaging Phone Number**
- A **Patient** may have at most one **Active Messaging Contact**
- A **Patient** without a successfully configured **Turn Contact** may have no **Active Messaging Contact**
- Bifrost remembers a **Patient**'s **Active Messaging Contact** independently of current prescription data
- A **Patient** gets an **Active Messaging Contact** through **Messaging Contact Activation**
- **Active Messaging Contact** identity is compared using normalized **Messaging Phone Number** values
- **Messaging Contact Activation** covers both first activation and activation after a **Messaging Phone Number Change**
- First **Messaging Contact Activation** happens before **Messaging Phone Number Change** handling for the same sync run
- First **Messaging Contact Activation** establishes the **Patient**'s **Active Messaging Contact**
- First **Messaging Contact Activation** may use the same welcome-trigger-only update shape as phone-change activation
- An already **Invite Sent** **Patient** may be treated as having their current **Messaging Phone Number** as their **Active Messaging Contact** without new welcome messaging
- A **Messaging Phone Number Change** belongs to exactly one **Patient**
- A **Messaging Phone Number Change** applies only after the **Patient** already has an **Active Messaging Contact**
- A **Messaging Phone Number Change** requires a replacement **Messaging Phone Number**
- A **Messaging Phone Number Change** replaces the **Active Messaging Contact** with another **Turn Contact** for that **Patient**
- A **Messaging Phone Number Change** makes the previous **Active Messaging Contact** a **Retired Messaging Contact**
- A **Messaging Phone Number Change** retires the previous **Active Messaging Contact** and activates the new **Turn Contact** as one messaging transition
- A **Messaging Phone Number Change** requires **Messaging Contact Activation** for the new **Turn Contact**
- A **Messaging Phone Number Change** retires the previous **Active Messaging Contact** even when another **Patient** may share that **Turn Contact**
- Retiring a **Turn Contact** after a **Messaging Phone Number Change** disables reminders without clearing historical patient or appointment context fields
- A **Messaging Phone Number Change** retires the previous **Active Messaging Contact** before welcome messaging is triggered on the new **Active Messaging Contact**
- A **Messaging Phone Number Change** is retryable until both the retirement and activation are accepted by the messaging system
- **Messaging Phone Number Change** success is tracked per **Patient**
- A **Patient**'s **Active Messaging Contact** changes only after the messaging system accepts the full **Messaging Phone Number Change**
- A **Retired Messaging Contact** may become an **Active Messaging Contact** again if the **Patient**'s **Messaging Phone Number** changes back to it
- A **Messaging Phone Number Change** does not erase **Invite Sent** for the **Patient**
- A **Messaging Phone Number Change** may require welcome messaging on the new **Active Messaging Contact** even when the **Patient** is already **Invite Sent**
- **Messaging Contact Activation** starts welcome messaging only after current patient and appointment context has been configured on the **Turn Contact**
- **Messaging Phone Number Change** handling depends on successful patient and appointment context refresh for the sync run
- **Messaging Contact Activation** may rely on a prior context refresh rather than repeating patient and appointment context in the welcome trigger update
- **Messaging Contact Activation** may use a batch-level trigger timestamp shared across multiple **Patient** records
- Welcome messaging on a new **Active Messaging Contact** is responsible for re-enabling reminders for that **Turn Contact**
- A **Patient** may map to different **Turn Contact** records over time as their **Messaging Phone Number** changes
- Multiple **Patient** records may map to the same **Turn Contact** when they share a **Messaging Phone Number**
- A **Patient** may be **New-Patient Eligible** even when they have no **Upcoming Appointment**
- A **Patient** is **New-Patient Eligible** only when they have a **Messaging Phone Number**
- A **Patient** is **New-Patient Eligible** only when they also have a usable **Facility**
- A **Patient** may become not **New-Patient Eligible** after being **Invite Sent**
- An **Upcoming Appointment** belongs to exactly one prescription
- An **Upcoming Appointment** must resolve to exactly one usable **Facility**
- A **Turn Contact** corresponds to exactly one WhatsApp phone number
- A **Turn Contact** may receive SyNCH and EDRWeb messaging fields when an **EDRWeb Patient** and a **Patient** share the same WhatsApp phone number
- A **Turn Contact** may hold the **SyNCH Patient Identifier** for the **Patient** currently associated with its phone number
- A **Turn Contact** can hold only one current **SyNCH Patient Identifier**, even when multiple **Patient** records share its **Messaging Phone Number**
- A **Consent Backfill** targets one or more **Turn Contact** records
- A **Consent Backfill** may set reminder-consent fields without changing whether a **Patient** is **Invite Sent**
- A **Consent Backfill** may use a CSV export as its source of facility text when production patient state is unavailable locally
- An **OTP Delivery Request** targets exactly one **Turn Contact**
- An **OTP Delivery Request** does not require a **Patient**
- An **OTP Delivery Request** must declare exactly one **OTP Recipient Type**
- **OTP Recipient Type** may be `patient` even when Bifrost cannot map the request to a stored **Patient**
- **OTP Recipient Type** is request-scoped and does not permanently classify a **Turn Contact**
- An **OTP Delivery Request** is authorised by exactly one **API Caller**
- **Delivery Protection** may reject an **OTP Delivery Request** even when the caller's OTP policy would otherwise allow it
- **Delivery Protection** for OTP sending is separate from **Reminder Suppressed**
- **Retired Messaging Contact** is separate from **Reminder Suppressed**
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
- "patient" was used for both SyNCH CCMDD records and EDRWeb `Persons` records — resolved: use **Patient** only for SyNCH CCMDD and **EDRWeb Patient** for EDRWeb
- EDRWeb and SyNCH patient records could be read as linkable clinical identities — resolved: Bifrost does not reconcile them because no shared upstream identifier is available
- `ccmdd_patient_id` and prescription `patient_id` could be read as competing identifiers — resolved: both represent the same **SyNCH Patient Identifier**
- adding patient identity to Turn could be read as resolving shared-phone ambiguity — resolved: shared **Messaging Phone Number** conflicts remain out of scope for this change
- "relevant prescription" could be read as Bifrost's own eligibility logic — resolved: use **Relevant Prescription Filter** for the upstream feed rule and keep local messaging decisions in Bifrost
- "latest phone number" was used to mean the newest stored phone value, even when unusable — resolved: messaging should use the most recent valid **Messaging Phone Number**
- missing or invalid current phone data could be read as a phone-number change — resolved: **Messaging Phone Number Change** requires a replacement **Messaging Phone Number**
- "user" was used for pre-sync OTP recipients — resolved: use **Turn Contact** for the recipient and **OTP Delivery Request** for the API call
- "user" was used for inbound authentication — resolved: use **API Caller** for the authenticated Django principal
- "patient vs user OTP" could be read as a delivery behavior split — resolved: use **OTP Recipient Type** for the business-role classification and keep current delivery behavior unchanged
- `429` could be read as OTP resend policy — resolved: use **Delivery Protection** for Bifrost-side channel safeguards and keep OTP lifecycle ownership in SyNCH
- reminder suppression could be read as a general messaging block — resolved: OTP **Delivery Protection** and **Reminder Suppressed** are separate mechanisms
- "opt in" was used to mean both clinic-captured consent and a WhatsApp button press — resolved: use **Consent Backfill** for the one-off operation that mirrors existing clinic consent into Turn
- Turn facility fields are normally derived from synced data, but this one-off reminder consent backfill may source the template facility name from the exported Turn CSV when local production sync data is unavailable
- echoed `msisdn` could be treated as the response identifier — resolved: prefer **Provider Message ID** over returning phone number in success responses
- upstream timeout could be read as a definite non-send — resolved: treat it as **Unknown Delivery Outcome**
- `invite_sent` could be read as current eligibility — resolved: **Invite Sent** is a one-way historical state
- "welcome message" could be read as only the first-ever patient invite — resolved: after a **Messaging Phone Number Change**, the new **Active Messaging Contact** may receive welcome messaging while **Invite Sent** remains a patient-level historical state
- "all contact fields" on the new **Turn Contact** could be read as Bifrost explicitly setting reminder consent — resolved: Bifrost sets the welcome trigger, and welcome messaging re-enables reminders on the new **Active Messaging Contact**
- retiring a shared **Turn Contact** could disable reminders for another **Patient** using the same **Messaging Phone Number** — resolved: shared-line protection is out of scope, and phone-change retirement still disables the old **Turn Contact**
- "opted out" was used for delivery-triggered reminder disabling — resolved: use **Reminder Suppressed** unless the user explicitly withdrew consent
