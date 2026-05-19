# OTP Delivery API

Bifrost exposes a synchronous HTTP API for sending a WhatsApp authentication template message through Turn.

## Endpoint

- `POST /api/v1/otp/send/`

## Authentication

Use Django REST Framework token authentication:

```http
Authorization: Token <token>
```

The authenticated Django user must have the `synch.send_otp` permission.

## Request body

Send JSON with:

- `msisdn`: required E.164 WhatsApp phone number
- `otp`: required non-empty string up to 15 characters
- `metadata`: optional object, accepted but ignored in v1

Example:

```json
{
  "msisdn": "+27831234567",
  "otp": "493821",
  "metadata": {
    "source": "sync-qa"
  }
}
```

## Success response

`200 OK`

```json
{
  "status": "submitted",
  "message_id": "wamid.123"
}
```

`message_id` is the provider-assigned identifier returned by Turn when the message is accepted.

## Error responses

- `400 Bad Request`: request validation failed
- `401 Unauthorized`: missing or invalid token
- `403 Forbidden`: authenticated caller lacks `synch.send_otp`
- `429 Too Many Requests`: per-contact delivery protection throttle
- `502 Bad Gateway`: non-timeout upstream Turn failure
- `504 Gateway Timeout`: timed out waiting for Turn; delivery outcome is unknown

Retries after `502` or `504` may duplicate delivery because Turn does not provide an idempotency mechanism for this flow.

## Configuration

This API uses these settings:

- `TURN_BASE_URL`
- `TURN_OTP_TOKEN`
- `TURN_OTP_TEMPLATE_NAMESPACE`
- `TURN_OTP_TEMPLATE_NAME`
- `TURN_OTP_TEMPLATE_LANGUAGE`
- `OTP_DELIVERY_THROTTLE_RATE` with default `6/h` using standard DRF rate syntax

The throttle state is stored in a dedicated Django database cache table created by migrations.

## Generated docs

- OpenAPI schema: `/api/schema/`
- Swagger UI: `/api/docs/`
