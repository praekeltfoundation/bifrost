from __future__ import annotations

from datetime import datetime
from typing import Any

from django.db import models


class EDRWebPatient(models.Model):
    patient_id: models.CharField[str, str] = models.CharField(
        max_length=255,
        unique=True,
    )
    phone_number: models.CharField[str, str] = models.CharField(
        max_length=255,
        blank=True,
    )
    updated_at: models.DateTimeField[datetime, datetime] = models.DateTimeField()
    appointments: models.JSONField[list[Any], list[Any]] = models.JSONField(
        default=list,
    )
    payload: models.JSONField[dict[str, Any], dict[str, Any]] = models.JSONField(
        default=dict,
    )

    def __str__(self) -> str:
        return self.patient_id
