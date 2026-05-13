# synch_delivery_failures

Standalone Turn.io Lua app for permanent WhatsApp delivery failures on SynCH contacts.

## Behavior

- Subscribes to delivery errors `131026` and `131050`.
- Sets `synch_reminders` to `"false"` when a supported delivery error arrives for a contact that is not yet suppressed.
- Records the first delivery-failure message ID in `synch_delivery_failure_message_id`.
- Backfills `synch_delivery_failure_message_id` when reminders are already suppressed but delivery-failure provenance is missing.
- Leaves the original `synch_delivery_failure_message_id` unchanged on later permanent failures.
- Processes every error in a batch and returns failure if any contact update still fails after retries.

## Local Development

Run commands from `turn_apps/synch_delivery_failures/`.

```bash
make test
make build
```

If you do not already have a local `turn-app` alias, these `make` targets use the official Turn Lua SDK container directly.

## Files

- `synch_delivery_failures.lua`: event handler
- `lib/delivery_failure_handler.lua`: delivery-failure class used by the root event router
- `spec/synch_delivery_failures_spec.lua`: app tests
- `assets/manifest.json`: app metadata and contact-field declaration
- `assets/README.md`: markdown shown in the Turn UI
