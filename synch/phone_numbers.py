import phonenumbers


def normalize_e164_phone_number(value: str) -> str | None:
    if not value.startswith("+"):
        return None

    try:
        phone_number = phonenumbers.parse(value, None)
    except phonenumbers.NumberParseException:
        return None

    if not phonenumbers.is_valid_number(phone_number):
        return None

    normalized_phone_number = phonenumbers.format_number(
        phone_number,
        phonenumbers.PhoneNumberFormat.E164,
    )
    if normalized_phone_number != value:
        return None

    return normalized_phone_number
