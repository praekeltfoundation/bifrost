# AGENTS.md - Turn.io Lua App Development Guide for AI Agents

This file provides context and instructions for AI coding agents working on synch_delivery_failures.

## Project Overview

This is a **Turn.io Lua App** - a sandboxed application that extends the Turn.io WhatsApp Business Platform through event-driven Lua scripts. Apps respond to events like contact changes, journey triggers, HTTP webhooks, and installation/configuration updates.

**Key Characteristics:**
- Event-driven architecture via `on_event(app, number, event, data)` function
- Sandboxed Lua 5.4 environment with restricted standard library
- Access to Turn.io APIs via `turn.*` modules (NOT standard Lua modules)
- ZIP-packaged deployment with manifest.json metadata
- Testing framework: lester (BDD-style, similar to RSpec/Jest)

## Development Environment

### Prerequisites
- Docker installed and running
- Turn.io Lua SDK image: `ghcr.io/turnhub/turn-lua-sdk:latest`

### Common Commands

```bash
# Run all tests
make test

# Watch mode - auto-run tests on file changes
make watch

# Build ZIP for deployment
make build

# Clean built artifacts
make clean

# Open interactive shell in container
make shell
```

**IMPORTANT:** Always run `make test` before completing any task. Fix all test failures.

## Code Conventions

### Lua Style Guidelines

1. **Naming Conventions:**
   - Variables/functions: `snake_case` (e.g., `contact_id`, `process_payment`)
   - Modules/Classes: `PascalCase` (e.g., `App`, `PaymentProcessor`)
   - Constants: `SCREAMING_SNAKE_CASE` (e.g., `MAX_RETRIES`, `API_VERSION`)

2. **Function Structure:**
   - Use descriptive names from business domain
   - Boolean checks: use `is_*` or `has_*` prefix (e.g., `is_valid`, `has_permission`)
   - Avoid single-letter variables except for standard iterators (`i`, `j`, `k`)

3. **Error Handling:**
   - Return multiple values: `success, error_message` pattern
   - Log errors with context: `turn.logger.error("Failed to X: " .. reason)`
   - Gracefully handle nil values - Lua doesn't throw on nil access

4. **Comments:**
   - Use `--` for single-line comments
   - Use `--[[  ]]` for multi-line comments
   - Document event handlers and non-obvious business logic

### Turn.io API Patterns

**DO:**
```lua
-- ✅ Correct: Access turn.json from turn module
local turn = require("turn")
local json = turn.json

-- ✅ Correct: Check for nil before accessing
if data and data.contact then
  local contact = data.contact
end

-- ✅ Correct: Return success/error tuple
local success, error = turn.contacts.update(contact_id, fields)
if not success then
  turn.logger.error("Update failed: " .. error)
  return false
end
```

**DON'T:**
```lua
-- ❌ Wrong: Can't require turn submodules directly
local json = require("turn.json")  -- This will fail!

-- ❌ Wrong: Assuming data structure without checks
local contact = data.contact  -- May be nil

-- ❌ Wrong: Using standard Lua I/O (sandboxed)
local file = io.open("data.txt", "r")  -- Not allowed!

-- ❌ Wrong: Using os.execute or similar
os.execute("curl https://example.com")  -- Sandboxed!
```

## Available Turn.io APIs

When writing code, you have access to these modules via `local turn = require("turn")`:

### Core APIs
- `turn.logger` - Logging (debug, info, warning, error)
- `turn.json` - JSON encoding/decoding
- `turn.http` - HTTP client for external requests
- `turn.assets` - Load files from app archive
- `turn.encoding` - Base64, URL-safe base64, hex encoding
- `turn.crypto` - HMAC, SHA256, secure random generation

### Business Logic APIs
- `turn.app` - App configuration, contact subscriptions, resource mapping
- `turn.contacts` - Contact search, update, field management
- `turn.journeys` - Journey/flow creation and management
- `turn.manifest` - Programmatic install/uninstall of contact fields, journeys, assets
- `turn.media` - Media file upload and management
- `turn.qrcode` - QR code generation
- `turn.leases` - Distributed resource leasing for async operations
- `turn.google` - Google Sheets integration

### Testing APIs (only in tests)
- `turn.test.reset()` - Reset state between tests
- `turn.test.mock_http()` - Mock HTTP requests
- `turn.test.mock_app_call()` - Mock app-to-app calls
- `turn.test.mock_journey_call()` - Mock journey triggers
- `turn.test.assert_http_called()` - Verify HTTP was called
- `turn.test.get_contact()` - Access test contacts
- `turn.test.create_contact()` - Create test data
- See `TESTING.md` for complete reference (~20 functions)

## Event Handler Patterns

Your main entry point is `App.on_event(app, number, event, data)`. Common events:

### Installation Events
```lua
if event == "install" then
  -- Set up contact field subscriptions
  turn.app.set_contact_subscriptions({"name", "email", "custom_field"})
  return true
end

if event == "uninstall" then
  -- Clean up subscriptions
  turn.app.set_contact_subscriptions({})
  return true
end
```

### Journey Integration Events
```lua
if event == "journey_event" then
  local function_name = data.function_name
  local args = data.args

  -- Synchronous response
  if function_name == "check_balance" then
    local balance = fetch_balance(args.account_id)
    return "continue", { balance = balance }
  end

  -- Asynchronous with lease
  if function_name == "process_payment" then
    local lease = turn.leases.acquire("payment_" .. args.transaction_id, 300)
    if not lease then
      return "error", "System busy, try again"
    end

    -- Process async, journey will wait for lease release
    return "wait", { lease_id = lease }
  end
end
```

### HTTP Webhook Events
```lua
if event == "http_request" then
  -- data.method = "GET" | "POST" | "PUT" | "DELETE"
  -- data.request_path = full request path string
  -- data.path_info = array of path segments
  -- data.query_string = raw query string
  -- data.query_params = table of parsed query params
  -- data.body_params = table of parsed body params
  -- data.params = table of merged query + body params
  -- data.req_headers = table of request headers

  if data.method == "POST" and data.path_info[1] == "webhook" then
    local payload = data.body_params
    -- Process webhook
    return true, {
      status = 200,
      headers = { ["content-type"] = "application/json" },
      body = turn.json.encode({ success = true })
    }
  end

  return true, { status = 404, body = "Not found" }
end
```

### Contact Change Events
```lua
if event == "contact_changed" then
  local contact = data.contact or data
  turn.logger.info("Contact " .. contact.uuid .. " updated")
  -- React to contact field changes
  return true
end
```

## Journey Integration with Apps

Turn.io journeys (conversation flows) can call your Lua app functions using the `app()` function in journey markdown files. This is a powerful integration pattern for building interactive WhatsApp experiences.

### How Journeys Call Apps

In journey markdown files, apps are invoked using:

```elixir
result = app("app_name", "function_name", [arg1, arg2, arg3])
```

Your app receives this as a `journey_event`:

```lua
if event == "journey_event" then
  local function_name = data.function_name  -- "function_name" from journey
  local args = data.args                    -- [arg1, arg2, arg3] from journey
  local chat_uuid = data.chat_uuid          -- Journey session ID

  if function_name == "get_available_slots" then
    local start_date = args[1]
    local end_date = args[2]
    local duration_minutes = args[3]

    -- Fetch available appointment slots
    local slots = fetch_slots_from_api(start_date, end_date, duration_minutes)

    -- Return data to journey
    return "continue", { available_slots = slots }
  end
end
```

### Journey Integration Patterns

#### Pattern 1: Data Retrieval (Synchronous)

Journey fetches data to display in a list or message:

```lua
-- Journey calls: app("fhir_app", "find_practitioners", ["cardiology"])
if function_name == "find_practitioners" then
  local specialty = args[1]

  turn.test.mock_http("fhir.example.com/Practitioner", {
    method = "GET",
    status = 200,
    body = json.encode({
      entry = {
        {resource = {id = "p1", name = "Dr. Smith"}},
        {resource = {id = "p2", name = "Dr. Jones"}}
      }
    })
  })

  local response = turn.http.request({
    url = "https://fhir.example.com/Practitioner?specialty=" .. specialty,
    method = "GET"
  })

  local practitioners = {}
  for _, entry in ipairs(json.decode(response.body).entry) do
    table.insert(practitioners, {
      id = entry.resource.id,
      name = entry.resource.name,
      display = entry.resource.name  -- Used by journey for list display
    })
  end

  return "continue", { practitioners = practitioners }
end
```

**Journey usage:**
```stack
card SelectPractitioner do
  # Call app to get practitioners
  result = app("fhir_app", "find_practitioners", ["cardiology"])

  # Map results to list options: [display_text, value]
  options = map(result.result.practitioners, &[&1.display, &1.id])

  # Show list to user
  practitioner_id = list("Select Doctor", NextCard, options) do
    text("Available cardiologists:")
  end
end
```

#### Pattern 2: Data Creation (Synchronous)

Journey collects data and creates a record:

```lua
-- Journey calls: app("fhir_app", "create_appointment", [json_spec])
if function_name == "create_appointment" then
  local appointment_spec = json.decode(args[1])

  -- Validate required fields
  if not appointment_spec.patientId then
    return "continue", { error = "Missing patientId" }
  end

  -- Create appointment via API
  local response = turn.http.request({
    url = "https://fhir.example.com/Appointment",
    method = "POST",
    headers = { ["content-type"] = "application/json" },
    body = json.encode({
      resourceType = "Appointment",
      status = "booked",
      start = appointment_spec.start_time,
      participant = {
        { actor = { reference = "Patient/" .. appointment_spec.patientId } }
      }
    })
  })

  if response.status == 201 then
    local appointment = json.decode(response.body)
    return "continue", {
      appointment_id = appointment.id,
      start_time = appointment.start,
      success = true
    }
  else
    return "continue", {
      error = "Failed to create appointment: " .. response.status
    }
  end
end
```

**Journey usage:**
```stack
card BookAppointment do
  # Call app to create appointment
  result = app("fhir_app", "create_appointment", [
    "{\"patientId\": \"@contact.patient_id\",
      \"start_time\": \"@contact.selected_slot_time\",
      \"practitioner_id\": \"@contact.selected_practitioner\"}"
  ])

  # Store appointment ID in contact
  update_contact(appointment_id: "@result.result.appointment_id")

  # Branch on success/error
  then(SuccessMessage when is_nil_or_empty(result.error))
  then(ErrorMessage when not is_nil_or_empty(result.error))
end
```

#### Pattern 3: Async Operations with Webhooks (Wait Signal)

Journey initiates payment and waits for webhook callback:

```lua
if function_name == "create_payment_intent" then
  local amount = args[1]
  local currency = args[2]
  local chat_uuid = data.chat_uuid

  -- Create payment session
  local response = turn.http.request({
    url = "https://api.stripe.com/v1/checkout/sessions",
    method = "POST",
    headers = {
      ["authorization"] = "Bearer " .. config.stripe_secret_key
    },
    body = "amount=" .. amount .. "&currency=" .. currency ..
           "&metadata[chat_uuid]=" .. chat_uuid
  })

  local session = json.decode(response.body)

  -- Return payment URL to journey
  return "continue", {
    payment_url = session.url,
    session_id = session.id
  }
end

if function_name == "wait_for_payment" then
  -- Journey will wait for webhook to call send_input()
  return "wait"
end

-- Later, when webhook arrives...
if event == "http_request" and data.path_info[1] == "webhook" then
  local webhook_data = data.body_params
  local chat_uuid = webhook_data.metadata.chat_uuid

  -- Resume journey with payment result
  turn.leases.send_input(chat_uuid, {
    payment_status = "completed",
    transaction_id = webhook_data.id
  })

  return true, { status = 200 }
end
```

**Journey usage:**
```stack
card CreatePayment do
  # Create payment intent
  payment = app("payment_app", "create_payment_intent", [2500, "usd"])

  # Send payment link to user
  text("Pay here: @payment.result.payment_url")

  then(WaitForPayment)
end

card WaitForPayment do
  # Wait for webhook (returns "wait" signal)
  app("payment_app", "wait_for_payment", [])

  # Journey pauses here until send_input() is called
  then(PaymentSuccess)
end

card PaymentSuccess do
  # Data from send_input() available as @input
  text("Payment completed! Transaction: @input.transaction_id")
end
```

#### Pattern 4: Complex Calculations

Journey collects inputs, app performs calculation:

```lua
if function_name == "calculate_diabetes_risk" then
  local age = tonumber(args[1])
  local gender = args[2]
  local family_history = args[3] == "yes"
  local bmi = tonumber(args[4])

  local score = 0

  if age > 45 then score = score + 2 end
  if family_history then score = score + 3 end
  if bmi > 25 then score = score + 2 end
  if gender == "male" then score = score + 1 end

  local risk_level = "low"
  if score > 6 then
    risk_level = "high"
  elseif score > 3 then
    risk_level = "medium"
  end

  return "continue", {
    score = score,
    risk_level = risk_level,
    recommendation = get_recommendation(risk_level)
  }
end
```

**Journey usage:**
```stack
card CalculateRisk do
  # Collect user inputs through previous cards
  # ref_Age, ref_Gender, ref_FamilyHistory, ref_BMI

  result = app("healthcare_app", "calculate_diabetes_risk", [
    "@ref_Age",
    "@ref_Gender",
    "@ref_FamilyHistory",
    "@ref_BMI"
  ])

  risk_score = result.result.score
  risk_level = result.result.risk_level

  then(HighRiskPath when risk_level == "high")
  then(MediumRiskPath when risk_level == "medium")
  then(LowRiskPath)
end
```

### Journey Data Interpolation

Journeys pass data to apps using string interpolation:

- `@contact.field_name` - Contact field value
- `@ref_VariableName` - Journey variable (from user input)
- `@global.app_name.setting` - Global app configuration
- `@json(value)` - JSON-safe encoding
- `@result.result.field` - Access app response data

### Testing Journey Integration

Always mock journey calls in tests:

```lua
it("should return available slots for journey", function()
  turn.test.reset()

  turn.test.mock_http("api.example.com/slots", {
    method = "GET",
    status = 200,
    body = json.encode({
      slots = {
        {start = "2024-01-15T09:00:00Z", end = "2024-01-15T09:30:00Z"},
        {start = "2024-01-15T10:00:00Z", end = "2024-01-15T10:30:00Z"}
      }
    })
  })

  local event_data = {
    function_name = "get_available_slots",
    args = {"2024-01-15", "2024-01-15", 30},
    chat_uuid = "test-chat-123"
  }

  local signal, payload = app:on_event({}, number, "journey_event", event_data)

  -- Test journey integration response
  assert(signal == "continue", "Expected continue signal")
  assert(type(payload.available_slots) == "table", "Expected slots array")
  assert(#payload.available_slots == 2, "Expected 2 slots")
  assert(payload.available_slots[1].display, "Expected display field for journey list")
end)

it("should handle async payment with wait signal", function()
  turn.test.set_config({ stripe_secret_key = "sk_test_123" })

  turn.test.mock_http("api.stripe.com/v1/checkout/sessions", {
    method = "POST",
    status = 200,
    body = json.encode({
      id = "cs_test_123",
      url = "https://checkout.stripe.com/pay/cs_test_123"
    })
  })

  -- Test payment creation
  local event_data = {
    function_name = "create_payment_intent",
    args = {2500, "usd"},
    chat_uuid = "test-chat-123"
  }

  local signal, payload = app:on_event({}, number, "journey_event", event_data)
  assert(signal == "continue", "Expected continue")
  assert(payload.payment_url, "Expected payment URL for journey")

  -- Test wait signal
  local wait_data = {
    function_name = "wait_for_payment",
    args = {},
    chat_uuid = "test-chat-123"
  }

  local wait_signal = app:on_event({}, number, "journey_event", wait_data)
  assert(wait_signal == "wait", "Expected wait signal for async operation")
end)
```

### Journey Integration Best Practices

1. **Always return proper signal** - `"continue"` for sync, `"wait"` for async, `"error"` for failures
2. **Include display fields** - Add `display` field to items used in journey lists
3. **Handle errors gracefully** - Return `{error = "message"}` instead of crashing
4. **Use JSON for complex data** - Pass JSON strings when sending structured data from journeys
5. **Test with journey_event** - Mock all journey integration points in tests
6. **Document function signatures** - Comment expected args for each journey function
7. **Validate inputs** - Check args before processing, return helpful errors
8. **Use descriptive function names** - Name functions based on business actions (create_appointment, not process_data)

## Testing Requirements

**CRITICAL:** All code changes must include tests. Tests live in `spec/` directory.

### Test Structure Pattern
```lua
local lester = require("lester")
local describe, it, before, after = lester.describe, lester.it, lester.before, lester.after

local turn = require("turn")
local json = turn.json

local app = require("synch_delivery_failures")
local number = { uuid = "test-number-uuid" }

describe("synch_delivery_failures", function()
  before(function()
    turn.test.reset()  -- ALWAYS reset state before each test
  end)

  it("should handle event", function()
    -- Arrange: Set up mocks
    turn.test.mock_http(
      {method = "GET", url = "https://api.example.com/data"},
      {status = 200, body = json.encode({result = "success"})}
    )

    -- Act: Call the function
    local result = app:on_event({}, number, "fetch_data", {})

    -- Assert: Verify behavior
    assert(result.success == true)
    turn.test.assert_http_called({method = "GET", url = "https://api.example.com/data"})
  end)
end)

return lester.report()
```

### Test Organization: Three-Level Nesting

Production apps use a consistent three-level structure:

```lua
describe("synch_delivery_failures", function()
  local app_config, number

  before(function()
    turn.test.reset()
    app_config = {config = {}, uuid = "app-uuid"}
    number = {id = "123", vname = "Test Number"}
  end)

  describe("Installation", function()
    it("should set up contact field subscriptions", function() end)
    it("should validate configuration on install", function() end)
  end)

  describe("Uninstallation", function()
    it("should remove only app-created journeys", function() end)
    it("should clear contact subscriptions", function() end)
  end)

  describe("Journey Events", function()
    describe("Payment Creation", function()
      it("should create payment intent with valid config", function() end)
      it("should handle missing API key gracefully", function() end)
      it("should include chat_uuid in payment metadata", function() end)
    end)

    describe("Payment Confirmation", function()
      it("should wait for webhook callback", function() end)
      it("should timeout after configured period", function() end)
    end)
  end)

  describe("HTTP Webhooks", function()
    it("should return 200 for valid webhook", function() end)
    it("should return 404 for unknown paths", function() end)
    it("should validate webhook signatures", function() end)
  end)

  describe("Error Handling", function()
    it("should handle unknown events gracefully", function() end)
    it("should handle API errors without crashing", function() end)
  end)
end)
```

### Testing Checklist

- [ ] Reset state with `turn.test.reset()` before each test
- [ ] Mock all external HTTP calls with `turn.test.mock_http()`
- [ ] Mock app-to-app calls with `turn.test.mock_app_call()`
- [ ] Verify calls with `turn.test.assert_http_called()` / `assert_app_called()`
- [ ] Test both success and error paths
- [ ] Use descriptive test names: `"should X when Y"`
- [ ] Include assertions that verify expected behavior
- [ ] Always test journey events return `signal, payload` (two values)
- [ ] Always test HTTP handlers return `success, response` (two values)
- [ ] Include edge case tests (nil values, empty arrays, malformed data)

### Critical Testing Patterns from Production Apps

#### Pattern: Signal-Based Assertions (Journey Events)

Journey event handlers return `signal, payload` - test both values:

```lua
it("should create payment and return URL", function()
  turn.test.mock_http("api.stripe.com/v1/checkout/sessions", {
    method = "POST",
    status = 200,
    body = json.encode({id = "cs_test", url = "https://checkout.stripe.com"})
  })

  local event_data = {
    function_name = "create_payment",
    args = {2500, "usd", "Test payment"},
    chat_uuid = "test-chat-123"
  }

  local signal, payload = app:on_event({}, number, "journey_event", event_data)

  -- ALWAYS test both signal and payload
  assert(signal == "continue", "Expected 'continue' signal")
  assert(type(payload) == "table", "Expected payload table")
  assert(payload.payment_url, "Expected payment_url in payload")
end)

it("should return error signal on API failure", function()
  turn.test.mock_http("api.stripe.com/v1/checkout/sessions", {
    method = "POST",
    status = 401,
    body = json.encode({error = {message = "Invalid API key"}})
  })

  local signal, payload = app:on_event({}, number, "journey_event", event_data)

  -- Error handling pattern
  assert(signal == "continue", "Continue even on error")
  assert(type(payload.error) == "string", "Expected error message in payload")
end)

it("should return wait signal for async operations", function()
  local event_data = {
    function_name = "wait_for_payment",
    args = {},
    chat_uuid = "test-chat-123"
  }

  local signal = app:on_event({}, number, "journey_event", event_data)

  -- Async pattern
  assert(signal == "wait", "Expected 'wait' signal for async operation")
end)
```

#### Pattern: HTTP Response Testing

HTTP request handlers return `success, response` - test status and headers:

```lua
it("should return 200 for valid webhook", function()
  local request_data = {
    method = "POST",
    path_info = {"webhook"},
    body = json.encode({event = "payment.success"}),
    req_headers = {}
  }

  local success, response = app:on_event({}, number, "http_request", request_data)

  assert(success == true, "Expected handler to succeed")
  assert(response.status == 200, "Expected 200 status")
  assert(response.headers["content-type"] == "application/json", "Expected JSON response")
end)

it("should return 404 for unknown paths", function()
  local request_data = {
    method = "GET",
    path_info = {"unknown"},
    req_headers = {}
  }

  local success, response = app:on_event({}, number, "http_request", request_data)

  assert(success == true, "Handler should succeed even for 404")
  assert(response.status == 404, "Expected 404 for unknown path")
end)
```

#### Pattern: Module Cache Clearing

For complex apps with multiple modules, clear package.loaded in after() hook:

```lua
describe("synch_delivery_failures", function()
  after(function()
    turn.test.reset()
    -- Clear all loaded modules to ensure fresh state
    package.loaded["synch_delivery_failures"] = nil
    package.loaded["synch_delivery_failures.utils"] = nil
    package.loaded["synch_delivery_failures.handlers"] = nil
  end)

  it("should load modules with fresh state", function()
    -- Module will be reloaded from scratch
    local app = require("synch_delivery_failures")
    -- Test with clean module state
  end)
end)
```

#### Pattern: Configuration Error Testing

Always test missing/invalid configuration:

```lua
it("should handle missing API key", function()
  -- Don't set any config
  turn.test.reset()

  local event_data = {
    function_name = "call_api",
    args = {},
    chat_uuid = "test-chat-123"
  }

  local signal, payload = app:on_event({}, number, "journey_event", event_data)

  -- Apps should continue with error in payload, not crash
  assert(signal == "continue", "Expected 'continue' signal")
  assert(type(payload.error) == "string", "Expected error message")
  assert(payload.error:match("configuration") or payload.error:match("API key"),
    "Expected configuration-related error")
end)

it("should validate required config fields", function()
  turn.test.set_config({
    base_url = "https://api.example.com"
    -- Missing required api_key
  })

  local event_data = {function_name = "test_function", args = {}}
  local signal, payload = app:on_event({}, number, "journey_event", event_data)

  assert(type(payload.error) == "string", "Expected error for incomplete config")
end)
```

#### Pattern: Edge Case Testing

Test boundary conditions and unusual inputs:

```lua
describe("Edge Cases", function()
  it("should handle empty args array", function()
    local event_data = {
      function_name = "my_function",
      args = {},  -- Empty args
      chat_uuid = "test-chat-123"
    }

    local signal, payload = app:on_event({}, number, "journey_event", event_data)
    assert(type(payload.error) == "string", "Expected error for missing args")
  end)

  it("should handle nil contact gracefully", function()
    local event_data = {
      contact = nil,  -- Missing contact
      -- ...
    }

    local signal, payload = app:on_event({}, number, "contact_changed", event_data)
    assert(signal ~= nil, "Should not crash on nil contact")
  end)

  it("should handle malformed JSON in webhook", function()
    local request_data = {
      method = "POST",
      path_info = {"webhook"},
      body = "{invalid json}",  -- Malformed
      req_headers = {}
    }

    local success, response = app:on_event({}, number, "http_request", request_data)
    assert(success == true, "Handler should not crash")
    assert(response.status >= 400, "Expected error status for bad JSON")
  end)
end)
```

### Common Testing Mistakes to Avoid

1. **Forgetting `turn.test.reset()`** - State leaks between tests
2. **Mocking after the call** - Mocks must be registered before code runs
3. **Not encoding JSON bodies** - Use `json.encode()` for body matching
4. **Missing query params in mock URLs** - Must match exactly
5. **Not testing error paths** - Always test failure scenarios
6. **Not clearing package.loaded** - Module state leaks in complex apps
7. **Assuming data structure** - Always check for nil before accessing nested fields
8. **Testing only success case** - Production apps test 3 paths: success, error, edge cases
9. **Not testing both signal AND payload** - Journey events return two values, test both
10. **Hardcoding test UUIDs** - Use descriptive test IDs like `"test-chat-123"`

## File Structure

```
synch_delivery_failures/
├── synch_delivery_failures.lua          # Main entry point (required)
├── manifest.json               # App metadata (required)
├── spec/
│   └── synch_delivery_failures_spec.lua # Tests (required)
├── assets/
│   ├── README.md               # User-facing docs (shown in Turn.io UI)
│   └── [other assets]          # Images, journey files, etc.
├── [module].lua                # Additional modules (optional)
├── Makefile                    # Build commands
├── TESTING.md                  # Testing reference for developers
├── AGENTS.md                   # This file - for AI agents
└── README.md                   # Developer documentation
```

## Manifest.json Structure

The `manifest.json` file is **required** and defines app metadata:

```json
{
  "app": {
    "type": "lua",
    "name": "synch_delivery_failures",
    "title": "Human Readable Name",
    "version": "0.1.0",
    "description": "What this app does",
    "dependencies": [],
    "functions": [
      {
        "name": "hello",
        "title": "Say Hello",
        "description": "Returns a greeting message",
        "parameters": {
          "type": "object",
          "properties": {
            "name": {
              "type": "string",
              "description": "Name to greet",
              "placeholder": "e.g. World",
              "label": "Name"
            }
          },
          "required": ["name"]
        },
        "returns": {
          "type": "object",
          "properties": {
            "message": { "type": "string", "description": "The greeting message" }
          }
        }
      }
    ]
  },
  "contact_fields": [
    {
      "type": "STRING",
      "name": "customer_id",
      "display": "Customer ID",
      "private": false
    }
  ],
  "journeys": [
    {
      "name": "onboarding_flow",
      "file": "assets/onboarding.md",
      "description": "Customer onboarding journey"
    }
  ],
  "media_assets": [
    {
      "asset_path": "assets/logo.png",
      "filename": "company_logo.png",
      "content_type": "image/png",
      "description": "Company logo"
    }
  ]
}
```

**Field Types:** `STRING`, `BOOLEAN`, `NUMBER`, `DATE`

**Function introspection:** Functions declared in `app.functions` describe your app's journey event handlers for the canvas UI. Each function specifies parameters (JSON Schema with label/placeholder), return fields, and branches (with `$RESULT` expressions). When `turn.manifest.install()` runs, function metadata is persisted to the database and served via GraphQL to the canvas, enabling typed parameter inputs and custom branch labels.

**When modifying manifest.json:**
- Increment `version` following semver
- Update `description` to reflect new functionality
- Add contact_fields if app creates custom contact data
- Add journeys if bundling conversation flows
- Add media_assets for images/documents used by app
- Add `app.functions` entries when adding journey event handlers (enables canvas UI introspection)

## Security Considerations

1. **Sandbox Restrictions:**
   - Cannot access filesystem (except via `turn.assets`)
   - Cannot execute system commands
   - Cannot require standard Lua modules (`io`, `os`, `debug`)
   - HTTP requests only via `turn.http`

2. **Secrets Management:**
   - Store API keys in app configuration (accessible via `turn.app.get_config()`)
   - Never hardcode secrets in code
   - Use `turn.crypto.hmac_*` for signature verification

3. **Input Validation:**
   - Always validate `data` parameters in `on_event`
   - Check for nil values before accessing nested fields
   - Validate HTTP webhook payloads before processing
   - Sanitize user input before using in external API calls

4. **Error Handling:**
   - Catch and log errors, don't let app crash
   - Return meaningful error messages
   - Use `pcall()` for operations that might fail

## Common Tasks

### Task: Add a new API integration
1. Add HTTP client code with `turn.http`
2. Mock HTTP calls in tests with `turn.test.mock_http()`
3. Add API key to configuration if needed
4. Handle error responses gracefully
5. Add tests for success and failure cases
6. Run `make test` to verify

### Task: Add a new journey event handler
1. Add new `function_name` case in `journey_event` handler
2. Define input args structure
3. Return `"continue", { result_data }` for sync
4. Return `"wait", { lease_id }` for async with leases
5. Return `"error", "message"` for failures
6. Mock journey calls in tests
7. Run `make test` to verify

### Task: Add contact field subscriptions
1. Define contact field in `manifest.json` under `contact_fields`
2. Add field name to `turn.app.set_contact_subscriptions()` in install event
3. Handle `contact_changed` event to react to changes
4. Add tests for contact change handling
5. Increment version in manifest.json
6. Run `make test` to verify

### Task: Debug failing tests
1. Check if `turn.test.reset()` is called before each test
2. Inspect HTTP requests: `turn.test.get_http_requests()`
3. Inspect app calls: `turn.test.get_app_calls()`
4. Add debug logging: `turn.logger.debug("Variable: " .. tostring(var))`
5. Run single test: `make test` (lester runs all tests in spec/)
6. Check test output for assertion failures
7. See `TESTING.md` for debugging patterns

## Documentation References

- **Full API Documentation:** https://whatsapp.turn.io/docs
- **Testing Reference:** See `TESTING.md` in this directory
- **User Documentation:** Edit `assets/README.md` for end-user docs
- **Get Support:** Contact Turn.io support on WhatsApp

## Workflow Summary for AI Agents

When asked to implement a feature:

1. **Understand the requirement** - Identify which event(s) need handling
2. **Check existing code** - Review current `on_event` implementation
3. **Write tests first** - Create test cases in `spec/` directory
4. **Implement feature** - Add/modify code in main app file
5. **Mock external dependencies** - Use `turn.test.mock_*` functions
6. **Run tests** - Execute `make test` and fix failures
7. **Update manifest** - Add fields/journeys/assets if needed
8. **Update documentation** - Modify `assets/README.md` for users
9. **Verify build** - Run `make build` to ensure ZIP creation works

**ALWAYS run `make test` before marking a task complete.**

## Common Pitfalls

1. **Incorrect module access:** Use `turn.json`, not `require("turn.json")`
2. **State leakage in tests:** Always call `turn.test.reset()` before each test
3. **Missing nil checks:** Lua doesn't throw on nil access, check explicitly
4. **Forgetting JSON encoding:** HTTP mock bodies must be strings, use `json.encode()`
5. **Not mocking external calls:** All `turn.http` calls must be mocked in tests
6. **Incorrect event names:** Event names are strings, check spelling exactly
7. **Missing manifest updates:** Update version and fields when adding functionality
8. **Not testing error paths:** Always test both success and failure scenarios

## Questions to Ask User

If requirements are unclear, ask:

- What event should trigger this functionality? (install, journey_event, http_request, etc.)
- Should this be synchronous (return immediately) or asynchronous (use leases)?
- What external APIs need to be called? (for mocking in tests)
- What contact fields are needed? (for manifest.json)
- What should happen on errors? (error handling strategy)
- Are there any security/validation requirements?

---

**This file is specifically for AI coding agents. For human developers, see README.md and TESTING.md.**
