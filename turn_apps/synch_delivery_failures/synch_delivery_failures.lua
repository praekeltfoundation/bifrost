local turn = require("turn")

local DeliveryFailureHandler = require("lib.delivery_failure_handler")

local App = {}
local handler = DeliveryFailureHandler.new()

function App.on_event(app, number, event, data)
    if event == "install" then
        return handler:install()
    end

    if event == "config_changed" then
        return handler:config_changed()
    end

    if event == "uninstall" then
        return handler:uninstall()
    end

    if event == "delivery_error" or event == "delivery_errors" then
        return handler:handle(data)
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
