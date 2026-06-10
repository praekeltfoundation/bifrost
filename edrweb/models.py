from __future__ import annotations

from datetime import datetime
from typing import Any

from django.db import models


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
            "WhatsApp phone number from EDRWeb PhoneNumber, usually in E.164 "
            "format. Blank means EDRWeb did not provide a phone number; future "
            "Turn sync cannot send reminders without one."
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
