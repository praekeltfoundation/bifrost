# SynCH delivery failures

This app listens for permanent WhatsApp delivery failures on the installed number and suppresses SynCH reminders for the affected contact.

## What it changes

- `synch_reminders` is set to `"false"` when Turn reports delivery error `131026` or `131050`.
- `synch_delivery_failure_message_id` stores the first failed outbound message ID that caused delivery-failure suppression.

## Notes

- The app keeps the first `synch_delivery_failure_message_id`; later delivery failures do not overwrite it.
- If reminders were already suppressed for another reason, the app backfills `synch_delivery_failure_message_id` when a supported delivery failure arrives.
- The app only updates the SynCH-owned reminder suppression fields. It does not switch channels or perform fallback delivery.
