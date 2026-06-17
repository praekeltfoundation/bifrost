from __future__ import annotations

from datetime import date, datetime
from typing import Any, cast

from django.db import models

from bifrost.phone_numbers import normalize_phone_number


class EDRWebPatient(models.Model):
    patient_id: models.CharField[str, str] = models.CharField(
        max_length=255,
        unique=True,
        help_text=(
            "Stable EDRWeb PersonId for this EDRWeb Patient. Used as the local "
            "upsert key for EDRWeb appointment reminder snapshots; not shared "
            "with SyNCH patient identifiers."
        ),
    )
    phone_number: models.CharField[str, str] = models.CharField(
        max_length=255,
        blank=True,
        help_text=(
            "Stored WhatsApp phone number from EDRWeb PhoneNumber, normalized "
            "to E.164. Blank means EDRWeb did not provide a usable phone number; "
            "future Turn sync cannot send reminders without one."
        ),
    )
    updated_at: models.DateTimeField[datetime, datetime] = models.DateTimeField(
        help_text=(
            "EDRWeb UpdatedAt timestamp for this snapshot. Bifrost uses the "
            "latest stored value as the next delta checkpoint, so do not set it "
            "in the future."
        ),
    )
    is_active: models.BooleanField[bool, bool] = models.BooleanField(
        default=True,
        help_text=(
            "Whether this EDRWeb Patient is currently present in the EDRWeb "
            "Appointment Reminder Feed. Inactive records should not drive active "
            "EDRWeb reminder messaging."
        ),
    )
    feed_removed_at: models.DateTimeField[datetime | None, datetime | None] = (
        models.DateTimeField(
            null=True,
            blank=True,
            help_text=(
                "Bifrost processing time when this EDRWeb Patient was marked "
                "absent from the full feed. Django admin manages this field from "
                "is_active: deactivating sets it, reactivating clears it."
            ),
        )
    )
    messaging_contact_activated: models.BooleanField[bool, bool] = models.BooleanField(
        default=False,
        help_text=(
            "Whether Turn has accepted the EDRWeb welcome-message activation "
            "trigger for the current active EDRWeb messaging phone number."
        ),
    )
    active_messaging_phone_number: models.CharField[str, str] = models.CharField(
        max_length=255,
        blank=True,
        help_text=(
            "Normalized WhatsApp phone number for the Turn contact Bifrost last "
            "activated for this EDRWeb Patient. Blank means no EDRWeb messaging "
            "contact activation has completed yet."
        ),
    )
    appointments: models.JSONField[list[Any], list[Any]] = models.JSONField(
        default=list,
        blank=True,
        help_text=(
            "Current EDRWeb Appointments array for reminder messaging. Each item "
            "must include AppointmentDate as YYYY-MM-DD and may include Facility "
            "with FacilityName, Latitude, and Longitude. Example: "
            '[{"AppointmentDate": "2026-06-20", "Facility": {"FacilityName": '
            '"WC BLUE DOWNS CLINIC", "Latitude": -33.9744, "Longitude": '
            "18.7032}}]."
        ),
    )
    payload: models.JSONField[dict[str, Any], dict[str, Any]] = models.JSONField(
        default=dict,
        help_text=(
            "Residual upstream EDRWeb fields not promoted into explicit model "
            "columns. Kept for inspection and troubleshooting; read-only in admin."
        ),
    )

    def __str__(self) -> str:
        return self.patient_id

    def save(self, *args: Any, **kwargs: Any) -> None:
        self.phone_number = normalize_phone_number(self.phone_number) or ""
        self.active_messaging_phone_number = (
            normalize_phone_number(self.active_messaging_phone_number) or ""
        )

        update_fields = kwargs.get("update_fields")
        if update_fields is not None:
            kwargs["update_fields"] = tuple(
                set(update_fields) | {"phone_number", "active_messaging_phone_number"}
            )

        super().save(*args, **kwargs)

    def get_turn_sync_row(self) -> dict[str, object] | None:
        if not self.phone_number:
            return None

        if not self.is_active:
            return {
                "urn": self.phone_number,
                "edrweb_reminders": "False",
            }

        row: dict[str, object] = {
            "urn": self.phone_number,
            "edrweb_patient_id": self.patient_id,
            "edrweb_next_appointment_date": "",
            "edrweb_appointment_facility_name": "",
            "edrweb_appointment_facility_latitude": "",
            "edrweb_appointment_facility_longitude": "",
        }
        appointment = self._get_earliest_appointment()
        if appointment is None:
            return row

        row["edrweb_next_appointment_date"] = appointment["AppointmentDate"]
        facility = appointment.get("Facility")
        if not isinstance(facility, dict):
            return row
        facility = cast(dict[str, object], facility)

        facility_name = facility.get("FacilityName")
        if isinstance(facility_name, str):
            row["edrweb_appointment_facility_name"] = facility_name
        latitude = facility.get("Latitude")
        if isinstance(latitude, int | float) and not isinstance(latitude, bool):
            row["edrweb_appointment_facility_latitude"] = latitude
        longitude = facility.get("Longitude")
        if isinstance(longitude, int | float) and not isinstance(longitude, bool):
            row["edrweb_appointment_facility_longitude"] = longitude

        return row

    def get_turn_activation_row(self, timestamp: str) -> dict[str, object] | None:
        if not self.phone_number:
            return None

        return {
            "urn": self.phone_number,
            "edrweb_new_user": timestamp,
        }

    def _get_earliest_appointment(self) -> dict[str, object] | None:
        appointments: list[tuple[date, dict[str, object]]] = []
        for item in self.appointments:
            if not isinstance(item, dict):
                continue
            appointment_date_value = item.get("AppointmentDate")
            if not isinstance(appointment_date_value, str):
                continue
            try:
                appointment_date = date.fromisoformat(appointment_date_value)
            except ValueError:
                continue
            appointments.append((appointment_date, item))

        if not appointments:
            return None

        return min(appointments, key=lambda item: item[0])[1]
