from __future__ import annotations

from datetime import date, datetime
from typing import cast

from django import forms
from django.contrib import admin
from django.core.exceptions import ValidationError
from django.http import HttpRequest
from django.utils import timezone as django_timezone

from edrweb.models import EDRWebPatient

ALLOWED_APPOINTMENT_KEYS = {"AppointmentDate", "Facility"}
ALLOWED_FACILITY_KEYS = {"FacilityName", "Latitude", "Longitude"}


def validate_appointments(appointments: object) -> list[dict[str, object]]:
    if not isinstance(appointments, list):
        raise ValidationError("Appointments must be a list of objects.")

    for index, item in enumerate(appointments, start=1):
        if not isinstance(item, dict):
            raise ValidationError(f"Appointment entry {index} must be an object.")

        item = cast(dict[str, object], item)
        unknown_keys = set(item) - ALLOWED_APPOINTMENT_KEYS
        if unknown_keys:
            raise ValidationError(
                f"Appointment entry {index} has unknown keys: "
                f"{', '.join(sorted(str(key) for key in unknown_keys))}."
            )

        appointment_date = item.get("AppointmentDate")
        if not isinstance(appointment_date, str):
            raise ValidationError(
                f"Appointment entry {index} must include an AppointmentDate string."
            )

        try:
            date.fromisoformat(appointment_date)
        except ValueError as error:
            raise ValidationError(
                f"Appointment entry {index} AppointmentDate must use YYYY-MM-DD format."
            ) from error

        facility = item.get("Facility")
        if facility is not None:
            validate_appointment_facility(facility, index)

    return cast(list[dict[str, object]], appointments)


def validate_appointment_facility(facility: object, appointment_index: int) -> None:
    if not isinstance(facility, dict):
        raise ValidationError(
            f"Appointment entry {appointment_index} Facility must be an object."
        )

    facility = cast(dict[str, object], facility)
    unknown_keys = set(facility) - ALLOWED_FACILITY_KEYS
    if unknown_keys:
        raise ValidationError(
            f"Appointment entry {appointment_index} Facility has unknown keys: "
            f"{', '.join(sorted(str(key) for key in unknown_keys))}."
        )

    facility_name = facility.get("FacilityName")
    if not isinstance(facility_name, str):
        raise ValidationError(
            f"Appointment entry {appointment_index} Facility must include a "
            "FacilityName string."
        )

    for coordinate_key in ("Latitude", "Longitude"):
        coordinate = facility.get(coordinate_key)
        if coordinate is not None and not _is_json_number(coordinate):
            raise ValidationError(
                f"Appointment entry {appointment_index} Facility {coordinate_key} "
                "must be a number."
            )


def _is_json_number(value: object) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool)


class EDRWebPatientAdminForm(forms.ModelForm):
    class Meta:
        model = EDRWebPatient
        fields = (
            "patient_id",
            "phone_number",
            "updated_at",
            "is_active",
            "appointments",
        )

    def clean_appointments(self) -> list[dict[str, object]]:
        return validate_appointments(self.cleaned_data["appointments"])

    def clean_updated_at(self) -> datetime:
        updated_at = cast(datetime, self.cleaned_data["updated_at"])
        if updated_at > django_timezone.now():
            raise ValidationError(
                "Updated at cannot be in the future because it is used as "
                "the EDRWeb delta checkpoint."
            )
        return updated_at


@admin.register(EDRWebPatient)
class EDRWebPatientAdmin(admin.ModelAdmin):
    form = EDRWebPatientAdminForm
    list_display = (
        "patient_id",
        "phone_number",
        "updated_at",
        "is_active",
        "messaging_contact_activated",
        "active_messaging_phone_number",
        "feed_removed_at",
    )
    search_fields = ("patient_id", "phone_number")
    readonly_fields = (
        "messaging_contact_activated",
        "active_messaging_phone_number",
        "feed_removed_at",
        "payload",
    )

    def save_model(
        self,
        request: HttpRequest,
        obj: EDRWebPatient,
        form: forms.ModelForm,
        change: bool,
    ) -> None:
        if obj.is_active:
            obj.feed_removed_at = None
        elif obj.feed_removed_at is None:
            obj.feed_removed_at = django_timezone.now()

        super().save_model(request, obj, form, change)
