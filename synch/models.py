from __future__ import annotations

from datetime import datetime
from typing import Any

from django.db import models


class Patient(models.Model):
    ccmdd_patient_id: models.CharField[str, str] = models.CharField(
        max_length=255,
        unique=True,
    )
    date_created: models.DateTimeField[datetime, datetime] = models.DateTimeField()
    date_updated: models.DateTimeField[datetime, datetime] = models.DateTimeField()
    payload: models.JSONField[dict[str, Any], dict[str, Any]] = models.JSONField(
        default=dict
    )

    def __str__(self) -> str:
        return self.ccmdd_patient_id


class Prescription(models.Model):
    ccmdd_prescription_id: models.CharField[str, str] = models.CharField(
        max_length=255,
        unique=True,
    )
    date_created: models.DateTimeField[datetime, datetime] = models.DateTimeField()
    date_updated: models.DateTimeField[datetime, datetime] = models.DateTimeField()
    facility_id: models.IntegerField[int, int] = models.IntegerField(
        null=True,
        blank=True,
    )
    patient_id: models.CharField[str, str] = models.CharField(max_length=255)
    patient_phone: models.CharField[str, str] = models.CharField(
        max_length=255,
        blank=True,
    )
    department_id: models.IntegerField[int, int] = models.IntegerField(
        null=True,
        blank=True,
    )
    return_dates: models.JSONField[list[dict[str, Any]], list[dict[str, Any]]] = (
        models.JSONField(default=list)
    )
    payload: models.JSONField[dict[str, Any], dict[str, Any]] = models.JSONField(
        default=dict
    )

    def __str__(self) -> str:
        return self.ccmdd_prescription_id
