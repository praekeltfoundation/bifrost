# Testing Guide for synch_delivery_failures

This guide provides quick reference for testing your Turn.io Lua app.

## Quick Start

```bash
make test           # Run all tests
make watch          # Run tests on file changes
```

## Testing API Reference

### State Management

```lua
-- Reset to clean state (2 contacts, 2 journeys)
turn.test.reset()

-- Set/get test configuration
turn.test.set_config("api_key", "test-key-123")
local key = turn.test.get_config("api_key")
```

### HTTP Request Mocking

```lua
-- Mock an HTTP request
turn.test.mock_http(
  {method = "GET", url = "https://api.example.com/data"},
  {status = 200, body = json.encode({result = "success"})}
)

-- Verify HTTP was called
turn.test.assert_http_called({
  method = "POST",
  url = "https://api.example.com/webhook",
  body = json.encode({event = "test"})
})

-- Get all HTTP requests made during test
local requests = turn.test.get_http_requests()
for _, req in ipairs(requests) do
  print(req.method, req.url)
end
```

### App-to-App Call Mocking

```lua
-- Mock a call to another app
turn.test.mock_app_call(
  "payment_app",
  "process_payment",
  {success = true, transaction_id = "txn_123"}
)

-- Verify app was called
turn.test.assert_app_called(
  "payment_app",
  "process_payment",
  {amount = 100, currency = "USD"}
)

-- Get all app calls
local calls = turn.test.get_app_calls()
```

### Journey Mocking

```lua
-- Mock a journey call
turn.test.mock_journey_call(
  "onboarding_flow",
  {completed = true, step = "final"}
)

-- Verify journey was triggered
turn.test.assert_journey_called("onboarding_flow", {
  contact_id = "contact-1",
  start_step = "welcome"
})
```

### Test Data Management

```lua
-- Pre-populated contacts (available after turn.test.reset())
local contact1 = turn.test.get_contact("contact-1")
local contact2 = turn.test.get_contact("contact-2")

-- Pre-populated journeys
local journey1 = turn.test.get_journey("journey-1")
local journey2 = turn.test.get_journey("journey-2")

-- Create custom test contact
local contact = turn.test.create_contact({
  name = "Test User",
  phone = "+1234567890",
  custom_field = "value"
})

-- Create custom test journey
local journey = turn.test.create_journey({
  name = "test_journey",
  description = "Test Journey"
})

-- Add test messages
turn.test.add_message(contact, "Hello", "inbound")
turn.test.add_message(contact, "Hi there!", "outbound")

-- Clear specific data
turn.test.clear_contacts()
turn.test.clear_journeys()
turn.test.clear_messages()
```

### Manifest Testing

```lua
-- Load and validate manifest
local manifest = turn.test.load_manifest()
assert(manifest.name == "synch_delivery_failures")

-- Check required fields
turn.test.assert_manifest_has("name")
turn.test.assert_manifest_has("version")
turn.test.assert_manifest_has("description")

-- Validate manifest structure
local valid, errors = turn.test.validate_manifest()
assert(valid, "Manifest validation failed: " .. json.encode(errors))
```

### Async Operations (Leases)

```lua
-- Test async operations with leases
it("should handle async processing", function()
  local lease_id = nil

  -- Mock lease acquisition
  turn.test.mock_lease_acquire("process_batch", function(id)
    lease_id = id
    return true
  end)

  -- Trigger async operation
  app:on_event({}, number, "process_batch", {items = 100})

  -- Verify lease was acquired
  assert(lease_id ~= nil, "Lease should be acquired")

  -- Verify lease is released
  turn.test.assert_lease_released(lease_id)
end)
```

## Common Testing Patterns

### Pattern 1: Testing Event Handlers

```lua
it("should handle install event", function()
  turn.test.reset()

  local result = app:on_event({}, number, "install", {})

  assert(result.success == true)
  -- Add your assertions
end)
```

### Pattern 2: Testing with HTTP API

```lua
it("should fetch external data", function()
  turn.test.reset()

  -- Mock the API response
  turn.test.mock_http(
    {method = "GET", url = "https://api.example.com/users/123"},
    {status = 200, body = json.encode({id = 123, name = "John"})}
  )

  local result = app:on_event({}, number, "fetch_user", {user_id = 123})

  -- Verify HTTP was called
  turn.test.assert_http_called({
    method = "GET",
    url = "https://api.example.com/users/123"
  })

  assert(result.user.name == "John")
end)
```

### Pattern 3: Testing Multi-App Integration

```lua
it("should communicate with other apps", function()
  turn.test.reset()

  -- Mock response from payment app
  turn.test.mock_app_call("payment_app", "check_balance", {
    balance = 1000,
    currency = "USD"
  })

  local result = app:on_event({}, number, "verify_funds", {amount = 100})

  -- Verify app call
  turn.test.assert_app_called("payment_app", "check_balance", {
    account_id = "acc_123"
  })

  assert(result.sufficient_funds == true)
end)
```

### Pattern 4: Testing Error Handling

```lua
it("should handle API errors gracefully", function()
  turn.test.reset()

  -- Mock API failure
  turn.test.mock_http(
    {method = "POST", url = "https://api.example.com/webhook"},
    {status = 500, body = "Internal Server Error"}
  )

  local result = app:on_event({}, number, "send_webhook", {data = "test"})

  assert(result.error ~= nil, "Should return error")
  assert(result.error:match("500"), "Should include status code")
end)
```

### Pattern 5: Testing Contact Updates

```lua
it("should update contact information", function()
  turn.test.reset()

  local contact = turn.test.get_contact("contact-1")

  app:on_event({}, number, "update_profile", {
    contact_id = contact.id,
    name = "Updated Name"
  })

  -- Verify contact was updated
  local updated = turn.test.get_contact("contact-1")
  assert(updated.name == "Updated Name")
end)
```

## Pre-Populated Test Data

After calling `turn.test.reset()`, the following test data is available:

### Contacts
- **contact-1**: Basic contact with phone number
- **contact-2**: Contact with additional fields

```lua
local contact1 = turn.test.get_contact("contact-1")
-- {id = "contact-1", phone = "+1234567890", name = "Test Contact 1"}
```

### Journeys
- **journey-1**: Simple journey template
- **journey-2**: Multi-step journey template

```lua
local journey1 = turn.test.get_journey("journey-1")
-- {name = "journey-1", description = "Test Journey 1"}
```

## Debugging Tips

### Enable Debug Logging

```lua
before(function()
  turn.test.reset()
  turn.test.set_log_level("debug")
end)
```

### Inspect HTTP Requests

```lua
after(function()
  local requests = turn.test.get_http_requests()
  for i, req in ipairs(requests) do
    print(string.format("Request %d: %s %s", i, req.method, req.url))
    print("Body:", req.body)
  end
end)
```

### Inspect App Calls

```lua
after(function()
  local calls = turn.test.get_app_calls()
  for i, call in ipairs(calls) do
    print(string.format("App call %d: %s.%s", i, call.app, call.event))
    print("Data:", json.encode(call.data))
  end
end)
```

### Verify State Between Tests

```lua
after(function()
  -- Ensure no leaked state
  local requests = turn.test.get_http_requests()
  assert(#requests == 0, "HTTP requests not cleared between tests")

  local calls = turn.test.get_app_calls()
  assert(#calls == 0, "App calls not cleared between tests")
end)
```

## Common Pitfalls

### 1. Forgetting to Reset State

```lua
-- ❌ BAD: State leaks between tests
it("test 1", function()
  turn.test.mock_http({...}, {...})
  -- test logic
end)

it("test 2", function()
  -- Still has mock from test 1!
end)

-- ✅ GOOD: Reset before each test
before(function()
  turn.test.reset()  -- Clears all mocks and state
end)
```

### 2. Incorrect HTTP Mock Matching

```lua
-- ❌ BAD: Mock won't match due to missing query params
turn.test.mock_http(
  {method = "GET", url = "https://api.example.com/data"},
  {status = 200, body = "{}"}
)
-- Actual call: GET https://api.example.com/data?page=1
-- This won't match!

-- ✅ GOOD: Include query params in mock
turn.test.mock_http(
  {method = "GET", url = "https://api.example.com/data?page=1"},
  {status = 200, body = "{}"}
)
```

### 3. Not Handling Async Operations

```lua
-- ❌ BAD: Doesn't wait for async operation
it("async test", function()
  app:on_event({}, number, "start_async", {})
  -- Assertion runs before async completes!
  assert(turn.test.get_result() ~= nil)
end)

-- ✅ GOOD: Use lease mocking to control async flow
it("async test", function()
  local completed = false
  turn.test.mock_lease_acquire("async_task", function()
    completed = true
    return true
  end)

  app:on_event({}, number, "start_async", {})
  assert(completed == true)
end)
```

### 4. Mocking After the Call

```lua
-- ❌ BAD: Mock registered after call
it("test", function()
  app:on_event({}, number, "fetch_data", {})
  turn.test.mock_http({...}, {...})  -- Too late!
end)

-- ✅ GOOD: Mock before the call
it("test", function()
  turn.test.mock_http({...}, {...})
  app:on_event({}, number, "fetch_data", {})
end)
```

### 5. Not Encoding JSON Bodies

```lua
-- ❌ BAD: Lua table won't match string body
turn.test.mock_http(
  {method = "POST", url = "https://api.example.com/data", body = {key = "value"}},
  {status = 200, body = "{}"}
)

-- ✅ GOOD: Encode body as JSON string
turn.test.mock_http(
  {method = "POST", url = "https://api.example.com/data",
   body = json.encode({key = "value"})},
  {status = 200, body = "{}"}
)
```

## Advanced Testing Techniques

### Testing with Configuration

```lua
describe("with API key", function()
  before(function()
    turn.test.reset()
    turn.test.set_config("api_key", "test-key-123")
  end)

  it("should authenticate requests", function()
    -- Test with config set
  end)
end)

describe("without API key", function()
  before(function()
    turn.test.reset()
    -- No config set
  end)

  it("should return auth error", function()
    -- Test without config
  end)
end)
```

### Testing Journey Integration

```lua
it("should trigger journey on event", function()
  turn.test.reset()

  turn.test.mock_journey_call("welcome_flow", {started = true})

  local contact = turn.test.get_contact("contact-1")
  app:on_event({}, number, "contact_created", {contact = contact})

  turn.test.assert_journey_called("welcome_flow", {
    contact_id = contact.id
  })
end)
```

### Testing Media Uploads

```lua
it("should upload media file", function()
  turn.test.reset()

  -- Mock media upload endpoint
  turn.test.mock_http(
    {method = "POST", url = "https://graph.facebook.com/v17.0/media"},
    {status = 200, body = json.encode({id = "media_123"})}
  )

  local result = app:on_event({}, number, "upload_image", {
    file = "test.jpg",
    data = "base64_encoded_data"
  })

  assert(result.media_id == "media_123")
end)
```

## Resources

- **Online Documentation**: https://whatsapp.turn.io/docs
- **Testing API Reference**: https://whatsapp.turn.io/docs#testing-api-reference
- **Get Support**: Contact us on WhatsApp for help with your app development

## Need Help?

- **This File**: You're reading it! Refer back to this TESTING.md for quick reference
- **Online Documentation**: https://whatsapp.turn.io/docs
- **Get Support**: Contact us on WhatsApp for help with your app development
