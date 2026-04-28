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
    invite_sent: models.BooleanField[bool, bool] = models.BooleanField(default=False)
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


class Facility(models.Model):
    ccmdd_facility_id: models.IntegerField[int, int] = models.IntegerField(unique=True)
    name: models.CharField[str, str] = models.CharField(max_length=255)
    latitude: models.CharField[str, str] = models.CharField(max_length=255, blank=True)
    longitude: models.CharField[str, str] = models.CharField(max_length=255, blank=True)
    telephone: models.CharField[str, str] = models.CharField(
        max_length=255,
        blank=True,
    )
    address_1: models.CharField[str, str] = models.CharField(
        max_length=255,
        blank=True,
    )
    address_2: models.CharField[str, str] = models.CharField(
        max_length=255,
        blank=True,
    )
    payload: models.JSONField[dict[str, Any], dict[str, Any]] = models.JSONField(
        default=dict
    )

    class Meta:
        verbose_name_plural = "facilities"

    def __str__(self) -> str:
        return self.name
