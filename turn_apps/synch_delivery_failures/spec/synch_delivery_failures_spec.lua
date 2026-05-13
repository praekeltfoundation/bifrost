local lester = require("lester")
local turn = require("turn")
local App = require("synch_delivery_failures")

local describe, it, before = lester.describe, lester.it, lester.before

describe("synch_delivery_failures", function()
    local app_config
    local number
    local subscription_result
    local update_results
    local loaded_assets
    local set_delivery_error_subscriptions_spy
    local update_contact_details_spy

    local function make_contact(fields)
        local copied_fields = {}
        for key, value in pairs(fields or {}) do
            copied_fields[key] = value
        end

        return {
            uuid = "contact-1",
            fields = copied_fields,
        }
    end

    local function make_delivery_error(overrides)
        local delivery_error = {
            code = 131026,
            status = "failed",
            message_id = "wamid-1",
            timestamp = "2026-05-12T12:00:00Z",
            contact = make_contact({}),
        }

        for key, value in pairs(overrides or {}) do
            delivery_error[key] = value
        end

        return delivery_error
    end

    local function assert_latest_contact_update(expected_details)
        local calls = update_contact_details_spy.calls
        local vals = calls[#calls].vals

        assert(vals[2] ~= nil)
        for key, value in pairs(expected_details) do
            assert(vals[2][key] == value)
        end

        for key, value in pairs(vals[2]) do
            assert(expected_details[key] == value)
        end
    end

    local function was_logged(level, pattern)
        -- Seems like the turn SDK doesn't have this documented function, so we need to implement it.
        local messages = turn.test.get_log_messages(level)
        for _, entry in ipairs(messages) do
            if string.match(entry.message, pattern) then
                return true
            end
        end

        return false
    end

    before(function()
        turn.test.reset()

        app_config = {
            uuid = "test-app-uuid",
            config = {},
        }

        number = {
            id = "123",
            vname = "+27820000000",
        }

        subscription_result = { success = true, reason = nil }
        update_results = {}
        loaded_assets = {
            ["README.md"] = "# SynCH delivery failures",
        }

        set_delivery_error_subscriptions_spy = turn.test.spy(function(codes)
            return subscription_result.success, subscription_result.reason
        end)
        turn.app.set_delivery_error_subscriptions = set_delivery_error_subscriptions_spy

        update_contact_details_spy = turn.test.spy(function(contact, details)
            local result = table.remove(update_results, 1)
            if result == nil then
                return {
                    uuid = contact.uuid,
                    fields = details,
                }, nil
            end

            if result.error ~= nil then
                return nil, result.error
            end

            return result.contact or {
                uuid = contact.uuid,
                fields = details,
            }, nil
        end)
        turn.contacts.update_contact_details = update_contact_details_spy

        turn.assets.load = function(path)
            return loaded_assets[path]
        end
    end)

    describe("subscription management", function()
        it("subscribes to the permanent delivery failure codes on install", function()
            local result = App.on_event(app_config, number, "install", {})
            local subscriptions = set_delivery_error_subscriptions_spy.calls[1].vals[1]

            assert(result == true)
            assert(#subscriptions == 2)
            assert(subscriptions[1] == 131026)
            assert(subscriptions[2] == 131050)
        end)

        it("reapplies the exact subscription set on config change", function()
            local result = App.on_event(app_config, number, "config_changed", {})
            local subscriptions = set_delivery_error_subscriptions_spy.calls[1].vals[1]

            assert(result == true)
            assert(#subscriptions == 2)
            assert(subscriptions[1] == 131026)
            assert(subscriptions[2] == 131050)
        end)

        it("fails install when subscription setup fails", function()
            subscription_result = {
                success = false,
                reason = "network error",
            }

            local result = App.on_event(app_config, number, "install", {})

            assert(result == false)
            assert(#turn.test.get_log_messages("error") == 1)
            assert(was_logged("error", "Failed to set delivery error subscriptions"))
        end)

        it("clears subscriptions on uninstall", function()
            local result = App.on_event(app_config, number, "uninstall", {})
            local subscriptions = set_delivery_error_subscriptions_spy.calls[1].vals[1]

            assert(result == true)
            assert(#subscriptions == 0)
        end)
    end)

    describe("delivery error handling", function()
        it("suppresses reminders and stores the first delivery failure message id", function()
            local result = App.on_event(app_config, number, "delivery_error", {
                errors = { make_delivery_error() },
            })

            assert(result == true)
            assert(update_contact_details_spy.call_count == 1)
            assert_latest_contact_update({
                synch_reminders = "false",
                synch_delivery_failure_message_id = "wamid-1",
            })
        end)

        it("treats false suppression values case-insensitively and backfills missing provenance", function()
            local result = App.on_event(app_config, number, "delivery_error", {
                errors = {
                    make_delivery_error({
                        contact = make_contact({
                            synch_reminders = "FALSE",
                            synch_delivery_failure_message_id = "",
                        }),
                    }),
                },
            })

            assert(result == true)
            assert(update_contact_details_spy.call_count == 1)
            assert_latest_contact_update({
                synch_delivery_failure_message_id = "wamid-1",
            })
        end)

        it("keeps the original provenance message id when the contact is not yet suppressed", function()
            local result = App.on_event(app_config, number, "delivery_error", {
                errors = {
                    make_delivery_error({
                        contact = make_contact({
                            synch_delivery_failure_message_id = "wamid-original",
                        }),
                    }),
                },
            })

            assert(result == true)
            assert(update_contact_details_spy.call_count == 1)
            assert_latest_contact_update({
                synch_reminders = "false",
            })
        end)

        it("no-ops when the contact is already suppressed and already has provenance", function()
            local result = App.on_event(app_config, number, "delivery_error", {
                errors = {
                    make_delivery_error({
                        contact = make_contact({
                            synch_reminders = "false",
                            synch_delivery_failure_message_id = "wamid-original",
                        }),
                    }),
                },
            })

            assert(result == true)
            assert(update_contact_details_spy.call_count == 0)
            assert(was_logged("info", "already has suppression and delivery%-failure provenance"))
        end)

        it("ignores unsupported delivery error codes", function()
            local result = App.on_event(app_config, number, "delivery_error", {
                errors = {
                    make_delivery_error({
                        code = 131999,
                    }),
                },
            })

            assert(result == true)
            assert(update_contact_details_spy.call_count == 0)
            assert(was_logged("info", "Ignoring unsupported delivery error code"))
        end)

        it("fails when contact data is missing", function()
            local result = App.on_event(app_config, number, "delivery_error", {
                errors = {
                    make_delivery_error({
                        contact = {},
                    }),
                },
            })

            assert(result == false)
            assert(update_contact_details_spy.call_count == 0)
            assert(#turn.test.get_log_messages("error") == 1)
        end)

        it("fails when message_id is missing", function()
            local result = App.on_event(app_config, number, "delivery_error", {
                errors = {
                    make_delivery_error({
                        message_id = "",
                    }),
                },
            })

            assert(result == false)
            assert(update_contact_details_spy.call_count == 0)
            assert(#turn.test.get_log_messages("error") == 1)
        end)

        it("retries contact updates up to three attempts", function()
            update_results = {
                { error = "temporary-1" },
                { error = "temporary-2" },
                { contact = { uuid = "contact-1" } },
            }

            local result = App.on_event(app_config, number, "delivery_error", {
                errors = { make_delivery_error() },
            })

            assert(result == true)
            assert(update_contact_details_spy.call_count == 3)
            assert(#turn.test.get_log_messages("warning") == 2)
        end)

        it("continues processing the batch and fails overall when any item fails", function()
            update_results = {
                { error = "temporary-1" },
                { error = "temporary-2" },
                { error = "temporary-3" },
                { contact = { uuid = "contact-2" } },
            }

            local result = App.on_event(app_config, number, "delivery_error", {
                errors = {
                    make_delivery_error({
                        contact = make_contact({}),
                        message_id = "wamid-1",
                    }),
                    make_delivery_error({
                        contact = {
                            uuid = "contact-2",
                            fields = {},
                        },
                        message_id = "wamid-2",
                    }),
                },
            })

            assert(result == false)
            assert(update_contact_details_spy.call_count == 4)
            assert(update_contact_details_spy.calls[4].vals[2].synch_delivery_failure_message_id == "wamid-2")
        end)

        it("fails when the payload does not contain an errors array", function()
            local result = App.on_event(app_config, number, "delivery_error", {})

            assert(result == false)
            assert(#turn.test.get_log_messages("error") == 1)
            assert(was_logged("error", "missing an errors array"))
        end)

        it("accepts the plural delivery_errors event name", function()
            local result = App.on_event(app_config, number, "delivery_errors", {
                errors = { make_delivery_error() },
            })

            assert(result == true)
            assert(update_contact_details_spy.call_count == 1)
        end)
    end)

    describe("documentation event", function()
        it("returns app info markdown from assets", function()
            local result = App.on_event(app_config, number, "get_app_info_markdown", {})
            assert(result == "# SynCH delivery failures")
        end)
    end)

    describe("core event compatibility", function()
        it("reapplies subscriptions on upgrade", function()
            local result = App.on_event(app_config, number, "upgrade", {})
            local subscriptions = set_delivery_error_subscriptions_spy.calls[1].vals[1]

            assert(result == true)
            assert(#subscriptions == 2)
            assert(subscriptions[1] == 131026)
            assert(subscriptions[2] == 131050)
        end)

        it("reapplies subscriptions on downgrade", function()
            local result = App.on_event(app_config, number, "downgrade", {})
            local subscriptions = set_delivery_error_subscriptions_spy.calls[1].vals[1]

            assert(result == true)
            assert(#subscriptions == 2)
            assert(subscriptions[1] == 131026)
            assert(subscriptions[2] == 131050)
        end)

        it("accepts contact field change events as a no-op", function()
            local result = App.on_event(app_config, number, "contact_fields_changed", {})

            assert(result == true)
            assert(set_delivery_error_subscriptions_spy.call_count == 0)
        end)

        it("returns a 404 response for unsupported http requests", function()
            local success, response = App.on_event(app_config, number, "http_request", {
                method = "GET",
                path_info = {},
            })

            assert(success == true)
            assert(response.status == 404)
            assert(response.body == "Not found")
        end)

        it("returns an explicit error for unsupported journey events", function()
            local signal, payload = App.on_event(app_config, number, "journey_event", {
                function_name = "unsupported",
                args = {},
            })

            assert(signal == "error")
            assert(payload == "Unsupported journey event")
        end)
    end)
end)
