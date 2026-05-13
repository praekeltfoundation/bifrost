local turn = require("turn")

local DeliveryFailureHandler = {}
DeliveryFailureHandler.__index = DeliveryFailureHandler

DeliveryFailureHandler.SUPPORTED_DELIVERY_ERROR_CODES = {
    131026,
    131050,
}

DeliveryFailureHandler.SYNCH_REMINDERS_FIELD = "synch_reminders"
DeliveryFailureHandler.DELIVERY_FAILURE_MESSAGE_ID_FIELD = "synch_delivery_failure_message_id"
DeliveryFailureHandler.UPDATE_RETRY_ATTEMPTS = 3

function DeliveryFailureHandler.new()
    return setmetatable({}, DeliveryFailureHandler)
end

function DeliveryFailureHandler:install()
    return self:_configure_delivery_error_subscriptions(
        self.SUPPORTED_DELIVERY_ERROR_CODES
    )
end

function DeliveryFailureHandler:config_changed()
    return self:_configure_delivery_error_subscriptions(
        self.SUPPORTED_DELIVERY_ERROR_CODES
    )
end

function DeliveryFailureHandler:uninstall()
    return self:_configure_delivery_error_subscriptions({})
end

function DeliveryFailureHandler:handle(data)
    if type(data) ~= "table" or type(data.errors) ~= "table" then
        turn.logger.error("Delivery error payload is missing an errors array")
        return false
    end

    local success = true

    for _, delivery_error in ipairs(data.errors) do
        if not self:_handle_delivery_error(delivery_error) then
            success = false
        end
    end

    return success
end

function DeliveryFailureHandler:_configure_delivery_error_subscriptions(codes)
    local success, reason = turn.app.set_delivery_error_subscriptions(codes)
    if not success then
        turn.logger.error("Failed to set delivery error subscriptions: " .. tostring(reason))
        return false
    end

    turn.logger.info("Configured delivery error subscriptions successfully")
    return true
end

function DeliveryFailureHandler:_handle_delivery_error(delivery_error)
    local code = delivery_error.code
    if not self:_is_supported_delivery_error_code(code) then
        turn.logger.info(
            string.format(
                "Ignoring unsupported delivery error code %s",
                tostring(code)
            )
        )
        return true
    end

    if not self:_validate_supported_delivery_error(delivery_error) then
        return false
    end

    local contact = delivery_error.contact
    local message_id = delivery_error.message_id
    local contact_uuid = self:_get_contact_uuid(contact)

    turn.logger.info(
        string.format(
            "Received delivery error for contact %s: code=%d, status=%s, message_id=%s, timestamp=%s",
            contact_uuid,
            code,
            tostring(delivery_error.status),
            message_id,
            tostring(delivery_error.timestamp)
        )
    )

    local details = self:_build_contact_update(contact, message_id)
    if details == nil then
        turn.logger.info(
            string.format(
                "Contact %s already has suppression and delivery-failure provenance; no update needed",
                contact_uuid
            )
        )
        return true
    end

    return self:_update_contact_with_retries(contact, message_id, details, code)
end

function DeliveryFailureHandler:_is_supported_delivery_error_code(code)
    for _, supported_code in ipairs(self.SUPPORTED_DELIVERY_ERROR_CODES) do
        if supported_code == code then
            return true
        end
    end

    return false
end

function DeliveryFailureHandler:_validate_supported_delivery_error(delivery_error)
    local contact = delivery_error.contact
    local contact_uuid = self:_get_contact_uuid(contact)
    if contact_uuid == nil then
        turn.logger.error("Delivery error is missing a usable contact payload")
        return false
    end

    if not self:_is_non_empty_string(delivery_error.message_id) then
        turn.logger.error(
            string.format(
                "Delivery error for contact %s is missing a message_id",
                contact_uuid
            )
        )
        return false
    end

    return true
end

function DeliveryFailureHandler:_update_contact_with_retries(contact, message_id, details, code)
    local contact_uuid = self:_get_contact_uuid(contact)

    for attempt = 1, self.UPDATE_RETRY_ATTEMPTS do
        local updated_contact, update_error = turn.contacts.update_contact_details(contact, details)
        if update_error == nil then
            turn.logger.info(
                string.format(
                    "Updated contact %s after delivery error %d attempt=%d result=%s",
                    contact_uuid,
                    code,
                    attempt,
                    turn.json.encode(updated_contact)
                )
            )
            return true
        end

        turn.logger.warning(
            string.format(
                "Attempt %d/%d failed for contact %s after delivery error %d with message_id=%s: %s",
                attempt,
                self.UPDATE_RETRY_ATTEMPTS,
                contact_uuid,
                code,
                message_id,
                tostring(update_error)
            )
        )
    end

    turn.logger.error(
        string.format(
            "Failed to update contact %s after delivery error %d with message_id=%s",
            contact_uuid,
            code,
            message_id
        )
    )
    return false
end

function DeliveryFailureHandler:_build_contact_update(contact, message_id)
    local details = {}

    if not self:_is_reminder_suppressed(contact) then
        details[self.SYNCH_REMINDERS_FIELD] = "false"
    end

    if self:_get_delivery_failure_message_id(contact) == nil then
        details[self.DELIVERY_FAILURE_MESSAGE_ID_FIELD] = message_id
    end

    if next(details) == nil then
        return nil
    end

    return details
end

function DeliveryFailureHandler:_is_reminder_suppressed(contact)
    local value = self:_get_contact_fields(contact)[self.SYNCH_REMINDERS_FIELD]
    return type(value) == "string" and string.lower(value) == "false"
end

function DeliveryFailureHandler:_get_delivery_failure_message_id(contact)
    local value = self:_get_contact_fields(contact)[self.DELIVERY_FAILURE_MESSAGE_ID_FIELD]
    if self:_is_non_empty_string(value) then
        return value
    end

    return nil
end

function DeliveryFailureHandler:_get_contact_fields(contact)
    if type(contact) ~= "table" or type(contact.fields) ~= "table" then
        return {}
    end

    return contact.fields
end

function DeliveryFailureHandler:_get_contact_uuid(contact)
    if type(contact) ~= "table" then
        return nil
    end

    local uuid = contact.uuid
    if self:_is_non_empty_string(uuid) then
        return uuid
    end

    return nil
end

function DeliveryFailureHandler:_is_non_empty_string(value)
    return type(value) == "string" and value ~= ""
end

return DeliveryFailureHandler
