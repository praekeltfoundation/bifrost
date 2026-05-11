from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import phonenumbers
from django.db import models


def _normalize_phone_number(value: str) -> str | None:
    try:
        phone_number = phonenumbers.parse(value, "ZA")
    except phonenumbers.NumberParseException:
        return None

    return phonenumbers.format_number(
        phone_number,
        phonenumbers.PhoneNumberFormat.E164,
    )


def _parse_return_date(value: object) -> date | None:
    if not isinstance(value, str):
        return None

    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


@dataclass(frozen=True)
class UpcomingAppointment:
    date: date
    prescription: Prescription
    facility: Facility


@dataclass(frozen=True)
class PatientTurnSyncDetails:
    messaging_phone_number: str | None
    upcoming_appointment: UpcomingAppointment | None


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

    @property
    def prescriptions(self) -> models.QuerySet[Prescription]:
        return Prescription.objects.filter(patient_id=self.ccmdd_patient_id).order_by(
            "date_created",
            "pk",
        )

    @property
    def messaging_phone_number(self) -> str | None:
        for prescription in self.prescriptions.order_by("-date_created", "-pk"):
            normalized_phone_number = _normalize_phone_number(
                prescription.patient_phone,
            )
            if normalized_phone_number is not None:
                return normalized_phone_number

        return None

    def get_upcoming_appointment(self, today: date) -> UpcomingAppointment | None:
        candidates: list[UpcomingAppointment] = []

        for prescription in self.prescriptions:
            facility = prescription.get_messaging_facility()
            if facility is None:
                continue

            for appointment_date in prescription.get_future_return_dates(today):
                candidates.append(
                    UpcomingAppointment(
                        date=appointment_date,
                        prescription=prescription,
                        facility=facility,
                    )
                )

        if not candidates:
            return None

        return min(
            candidates,
            key=lambda candidate: (
                candidate.date,
                -candidate.prescription.date_created.timestamp(),
                -candidate.prescription.pk,
            ),
        )

    def get_turn_sync_details(self, today: date) -> PatientTurnSyncDetails:
        return PatientTurnSyncDetails(
            messaging_phone_number=self.messaging_phone_number,
            upcoming_appointment=self.get_upcoming_appointment(today),
        )


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

    @property
    def normalized_patient_phone(self) -> str | None:
        return _normalize_phone_number(self.patient_phone)

    def get_future_return_dates(self, today: date) -> list[date]:
        appointment_dates: list[date] = []

        for return_date in self.return_dates:
            if not isinstance(return_date, dict):
                continue

            appointment_date = _parse_return_date(return_date.get("return_date"))
            if appointment_date is None or appointment_date < today:
                continue

            appointment_dates.append(appointment_date)

        return appointment_dates

    def get_messaging_facility(self) -> Facility | None:
        if self.facility_id is None:
            return None

        facility = Facility.objects.filter(ccmdd_facility_id=self.facility_id).first()
        if facility is None or not facility.is_usable_for_messaging:
            return None

        return facility


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

    @property
    def is_usable_for_messaging(self) -> bool:
        return bool(self.name.strip())
