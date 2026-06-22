# SyNCH Appointment Reminder Logic

This document describes when Bifrost sends SyNCH appointment reminder context to
Turn, how it keeps an appointment active after the appointment date, and when it
moves on after a missed appointment.

## Reminder Fields

Bifrost does not send reminders directly. It refreshes Turn contact fields that
Turn journeys use for reminder messaging:

- `synch_patient_id`
- `synch_next_appointment_date`
- `synch_appointment_facility_name`
- `synch_appointment_facility_latitude`
- `synch_appointment_facility_longitude`

The legacy field name `synch_next_appointment_date` may contain a past date. In
domain terms it contains the current **Tracked Appointment** date, not
necessarily the next future appointment.

## Appointment Reminder Conditions

A patient can receive appointment reminders when all of these are true:

- The **Patient** has a usable **Messaging Phone Number**.
- Bifrost can find a usable **Tracked Appointment**.
- The **Tracked Appointment** resolves to a usable **Facility**.
- The Turn contact has not otherwise been disabled for SyNCH reminders.

A usable **Tracked Appointment** is selected from prescription `return_dates`.
Bifrost:

- reads all prescriptions for the same **Patient**
- ignores invalid return-date entries
- ignores appointment dates whose prescription has no usable **Facility**
- ignores appointment dates already resolved by a **Related Prescription**
- ignores appointment dates that have become **Missed Appointment** records
- selects the earliest remaining appointment date
- breaks ties by choosing the most recently created prescription

When a usable **Tracked Appointment** exists, Bifrost sends its date and its
prescription's facility to Turn.

## Related Prescriptions

A **Related Prescription** is the signal that the patient went for an
appointment.

A prescription is related to an appointment when:

- it belongs to the same **Patient**
- its creation date is from 14 days before through 56 days after the appointment
  date, inclusive
- it is not the same prescription that carried that appointment date

Related-prescription matching uses prescription `date_created`, not
`date_updated`. The stored prescription creation calendar date is used as the
source date.

One prescription can resolve more than one appointment when its creation date is
inside multiple appointment windows.

After a **Related Prescription** resolves an appointment, Bifrost stops sending
that appointment date. If the related prescription contains another usable return
date, that later date can become the next **Tracked Appointment**.

## Missed Appointment Reminder Conditions

A patient can receive missed appointment reminders when all of these are true:

- The **Patient** still has a usable **Messaging Phone Number**.
- A **Tracked Appointment** date has passed.
- No **Related Prescription** exists for that appointment yet.
- The appointment is still inside its related-prescription window.
- The Turn contact has not otherwise been disabled for SyNCH reminders.

In this state, Bifrost keeps sending the same appointment date to Turn. Turn can
then treat the date as overdue and send missed appointment reminder messaging.

Example:

- appointment date: `2026-05-01`
- today: `2026-05-20`
- no related prescription exists

Bifrost still sends:

```text
synch_next_appointment_date = 2026-05-01
```

## When Missed Appointment Reminders Stop

The related-prescription window ends 56 days after the appointment date.

The appointment remains active through that final window day. It becomes a
**Missed Appointment** on the next calendar day if no **Related Prescription**
exists.

Example:

- appointment date: `2026-05-01`
- final related-prescription day: `2026-06-26`
- on `2026-06-26`, Bifrost still sends `2026-05-01`
- on `2026-06-27`, Bifrost treats the appointment as missed and stops sending
  it

After an appointment becomes missed, Bifrost moves to the next unresolved usable
appointment when one exists. If none exists, Bifrost clears
`synch_next_appointment_date`.

## Facility Fallback

When no usable **Tracked Appointment** exists, Bifrost clears the appointment
date field but may still send facility fields.

Facility fallback uses the most recently created prescription whose facility is
usable for messaging. This keeps invite and general patient context usable even
when no appointment date can drive reminders.
