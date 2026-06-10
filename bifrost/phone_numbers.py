import phonenumbers


def normalize_phone_number(value: str) -> str | None:
    try:
        phone_number = phonenumbers.parse(value, "ZA")
    except phonenumbers.NumberParseException:
        return None

    return phonenumbers.format_number(
        phone_number,
        phonenumbers.PhoneNumberFormat.E164,
    )
