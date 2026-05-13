local turn = require("turn")

local DeliveryFailureHandler = require("lib.delivery_failure_handler")

local App = {}
local handler = DeliveryFailureHandler.new()

function App.on_event(app, number, event, data)
    if event == "install" or event == "config_changed" or event == "upgrade" or event == "downgrade" then
        return handler:subscribe_to_delivery_errors()
    end

    if event == "uninstall" then
        return handler:clear_delivery_error_subscriptions()
    end

    if event == "contact_fields_changed" then
        return true
    end

    if event == "delivery_error" or event == "delivery_errors" then
        return handler:handle(data)
    end

    if event == "http_request" then
        return true, {
            status = 404,
            body = "Not found",
        }
    end

    if event == "journey_event" then
        return "error", "Unsupported journey event"
    end

    if event == "get_app_info_markdown" then
        local readme = turn.assets.load("README.md")
        if readme then
            return readme
        end

        return "# synch_delivery_failures\n\nSee the repository README for usage."
    end

    turn.logger.warning("Received unhandled event: " .. tostring(event))
    return false
end

return App
