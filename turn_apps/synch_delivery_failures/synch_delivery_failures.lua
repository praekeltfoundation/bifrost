local turn = require("turn")

local DeliveryFailureHandler = require("lib.delivery_failure_handler")

local App = {}
local handler = DeliveryFailureHandler.new()

local function load_manifest()
    local manifest_json = turn.assets.load("manifest.json")
    if manifest_json == nil then
        turn.logger.error("Failed to load manifest.json")
        return nil
    end

    return turn.json.decode(manifest_json)
end

local function install_manifest()
    local manifest = load_manifest()
    if manifest == nil then
        return false
    end

    local result = turn.manifest.install(manifest)
    return result ~= nil and result.success == true
end

local function uninstall_manifest()
    local manifest = load_manifest()
    if manifest == nil then
        return false
    end

    local result = turn.manifest.uninstall(manifest)
    return result ~= nil and result.success == true
end

function App.on_event(app, number, event, data)
    if event == "install" or event == "config_changed" or event == "upgrade" or event == "downgrade" then
        if not install_manifest() then
            return false
        end

        return handler:subscribe_to_delivery_errors()
    end

    if event == "uninstall" then
        if not uninstall_manifest() then
            return false
        end

        return handler:clear_delivery_error_subscriptions()
    end

    if event == "contact_fields_changed" or event == "contact_changed" or event == "worker_init" then
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
