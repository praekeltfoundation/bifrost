from __future__ import annotations

from datetime import date
from typing import cast

from django.contrib import admin
from django.core.exceptions import ValidationError

from synch.models import Facility, Patient, Prescription

ALLOWED_RETURN_DATE_KEYS = {
    "return_date",
    "note",
    "day_count",
    "description",
}


def validate_return_dates(return_dates: object) -> list[dict[str, object]]:
    if not isinstance(return_dates, list):
        raise ValidationError("Return dates must be a list of objects.")

    for index, item in enumerate(return_dates, start=1):
        if not isinstance(item, dict):
            raise ValidationError(f"Return date entry {index} must be an object.")

        item = cast(dict[str, object], item)
        unknown_keys = set(item) - ALLOWED_RETURN_DATE_KEYS
        if unknown_keys:
            raise ValidationError(
                f"Return date entry {index} has unknown keys: "
                f"{', '.join(sorted(unknown_keys))}."
            )

        return_date = item.get("return_date")
        if not isinstance(return_date, str):
            raise ValidationError(
                f"Return date entry {index} must include a return_date string."
            )

        try:
            date.fromisoformat(return_date)
        except ValueError as error:
            raise ValidationError(
                f"Return date entry {index} must use YYYY-MM-DD format."
            ) from error

    return cast(list[dict[str, object]], return_dates)


@admin.register(Patient)
class PatientAdmin(admin.ModelAdmin):
    list_display = ("ccmdd_patient_id", "date_created", "date_updated")
    search_fields = ("ccmdd_patient_id",)
    readonly_fields = (
        "invite_sent",
        "active_messaging_phone_number",
        "turn_appointment_context_urn",
        "turn_appointment_context_patient_id",
        "turn_appointment_context_next_appointment_date",
        "turn_appointment_context_facility_name",
        "turn_appointment_context_facility_latitude",
        "turn_appointment_context_facility_longitude",
        "turn_appointment_context_synced_at",
        "payload",
    )


@admin.register(Prescription)
class PrescriptionAdmin(admin.ModelAdmin):
    list_display = (
        "ccmdd_prescription_id",
        "patient_id",
        "date_created",
        "date_updated",
    )
    search_fields = ("ccmdd_prescription_id", "patient_id")
    readonly_fields = ("payload",)

    def get_form(self, request, obj=None, change=False, **kwargs):
        form_class = super().get_form(request, obj, change=change, **kwargs)

        class PrescriptionAdminForm(form_class):  # ty: ignore[unsupported-base]
            def clean_return_dates(self) -> list[dict[str, object]]:
                return validate_return_dates(self.cleaned_data["return_dates"])

        return PrescriptionAdminForm


@admin.register(Facility)
class FacilityAdmin(admin.ModelAdmin):
    list_display = ("ccmdd_facility_id", "name", "telephone")
    search_fields = ("ccmdd_facility_id", "name")
    readonly_fields = ("payload",)
