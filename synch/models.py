from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from django.contrib.auth.models import User
from django.db import models

from bifrost.phone_numbers import normalize_phone_number


def _parse_return_date(value: object) -> date | None:
    if not isinstance(value, str):
        return None

    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _parse_coordinate(value: str) -> Decimal | None:
    if not value:
        return None

    try:
        return Decimal(value)
    except InvalidOperation:
        return None


@dataclass(frozen=True)
class TrackedAppointment:
    date: date
    prescription: Prescription
    facility: Facility


@dataclass(frozen=True)
class PatientTurnSyncDetails:
    patient_id: str
    messaging_phone_number: str | None
    tracked_appointment: TrackedAppointment | None
    messaging_facility: Facility | None

    def get_turn_appointment_import_row(self) -> dict[str, object]:
        next_appointment_date = self._get_next_appointment_date()
        facility_name, facility_latitude, facility_longitude = (
            self._get_facility_values()
        )
        return {
            "urn": self.messaging_phone_number,
            "synch_patient_id": self.patient_id,
            "synch_next_appointment_date": (
                next_appointment_date.isoformat() if next_appointment_date else ""
            ),
            "synch_appointment_facility_name": facility_name,
            "synch_appointment_facility_latitude": facility_latitude,
            "synch_appointment_facility_longitude": facility_longitude,
        }

    def get_turn_appointment_context_fields(self) -> dict[str, object]:
        facility_name, facility_latitude, facility_longitude = (
            self._get_facility_values()
        )
        return {
            "turn_appointment_context_urn": self.messaging_phone_number,
            "turn_appointment_context_patient_id": self.patient_id,
            "turn_appointment_context_next_appointment_date": (
                self._get_next_appointment_date()
            ),
            "turn_appointment_context_facility_name": facility_name,
            "turn_appointment_context_facility_latitude": _parse_coordinate(
                facility_latitude
            ),
            "turn_appointment_context_facility_longitude": _parse_coordinate(
                facility_longitude
            ),
        }

    def _get_next_appointment_date(self) -> date | None:
        if self.tracked_appointment is None:
            return None
        return self.tracked_appointment.date

    def _get_facility_values(self) -> tuple[str, str, str]:
        if self.messaging_facility is None:
            return "", "", ""
        return (
            self.messaging_facility.name,
            self.messaging_facility.latitude,
            self.messaging_facility.longitude,
        )


class Patient(models.Model):
    ccmdd_patient_id: models.CharField[str, str] = models.CharField(
        max_length=255,
        unique=True,
    )
    date_created: models.DateTimeField[datetime, datetime] = models.DateTimeField()
    date_updated: models.DateTimeField[datetime, datetime] = models.DateTimeField()
    invite_sent: models.BooleanField[bool, bool] = models.BooleanField(default=False)
    active_messaging_phone_number: models.CharField[str, str] = models.CharField(
        max_length=255,
        blank=True,
    )
    turn_appointment_context_urn: models.CharField[str, str] = models.CharField(
        max_length=255,
        blank=True,
    )
    turn_appointment_context_patient_id: models.CharField[str, str] = models.CharField(
        max_length=255, blank=True
    )
    turn_appointment_context_next_appointment_date: models.DateField[
        date | None, date | None
    ] = models.DateField(null=True, blank=True)
    turn_appointment_context_facility_name: models.CharField[str, str] = (
        models.CharField(max_length=255, blank=True)
    )
    turn_appointment_context_facility_latitude: models.DecimalField[
        Decimal | None, Decimal | None
    ] = models.DecimalField(
        max_digits=10,
        decimal_places=7,
        null=True,
        blank=True,
    )
    turn_appointment_context_facility_longitude: models.DecimalField[
        Decimal | None, Decimal | None
    ] = models.DecimalField(
        max_digits=10,
        decimal_places=7,
        null=True,
        blank=True,
    )
    turn_appointment_context_synced_at: models.DateTimeField[
        datetime | None, datetime | None
    ] = models.DateTimeField(null=True, blank=True)
    payload: models.JSONField[dict[str, Any], dict[str, Any]] = models.JSONField(
        default=dict
    )

    def __str__(self) -> str:
        return self.ccmdd_patient_id

    def get_turn_sync_details(
        self,
        *,
        today: date,
        prescriptions: list[Prescription],
        facilities_by_id: dict[int, Facility],
    ) -> PatientTurnSyncDetails:
        tracked_appointment = self._get_tracked_appointment_from_prescriptions(
            today,
            prescriptions,
            facilities_by_id,
        )
        return PatientTurnSyncDetails(
            patient_id=self.ccmdd_patient_id,
            messaging_phone_number=self._get_messaging_phone_number_from_prescriptions(
                prescriptions,
            ),
            tracked_appointment=tracked_appointment,
            messaging_facility=self._get_messaging_facility_for_turn_sync(
                prescriptions,
                facilities_by_id,
                tracked_appointment,
            ),
        )

    def _get_messaging_phone_number_from_prescriptions(
        self,
        prescriptions: list[Prescription],
    ) -> str | None:
        for prescription in reversed(prescriptions):
            normalized_phone_number = normalize_phone_number(
                prescription.patient_phone,
            )
            if normalized_phone_number is not None:
                return normalized_phone_number

        return None

    def _get_tracked_appointment_from_prescriptions(
        self,
        today: date,
        prescriptions: list[Prescription],
        facilities_by_id: dict[int, Facility],
    ) -> TrackedAppointment | None:
        candidates: list[TrackedAppointment] = []

        for prescription in prescriptions:
            facility = prescription.get_messaging_facility(
                facilities_by_id=facilities_by_id
            )
            if facility is None:
                continue

            for appointment_date in prescription.get_trackable_return_dates(today):
                if self._has_related_prescription(
                    appointment_date=appointment_date,
                    appointment_prescription=prescription,
                    prescriptions=prescriptions,
                ):
                    continue

                candidates.append(
                    TrackedAppointment(
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

    def _has_related_prescription(
        self,
        *,
        appointment_date: date,
        appointment_prescription: Prescription,
        prescriptions: list[Prescription],
    ) -> bool:
        window_start = appointment_date - timedelta(weeks=2)
        window_end = appointment_date + timedelta(weeks=8)

        for prescription in prescriptions:
            if prescription == appointment_prescription:
                continue

            prescription_date = prescription.date_created.date()
            if window_start <= prescription_date <= window_end:
                return True

        return False

    def _get_messaging_facility_for_turn_sync(
        self,
        prescriptions: list[Prescription],
        facilities_by_id: dict[int, Facility],
        tracked_appointment: TrackedAppointment | None,
    ) -> Facility | None:
        if tracked_appointment is not None:
            return tracked_appointment.facility

        for prescription in reversed(prescriptions):
            facility = prescription.get_messaging_facility(
                facilities_by_id=facilities_by_id
            )
            if facility is not None:
                return facility

        return None


class APIUser(User):
    class Meta:
        proxy = True
        permissions = (("send_otp", "Can send OTP delivery requests"),)
        verbose_name = "API user"
        verbose_name_plural = "API users"


class Prescription(models.Model):
    ccmdd_prescription_id: models.CharField[str, str] = models.CharField(
        max_length=255,
        unique=True,
    )
    date_created: models.DateTimeField[datetime, datetime] = models.DateTimeField()
    date_updated: models.DateTimeField[datetime, datetime] = models.DateTimeField()
    facility_id: models.IntegerField[int | None, int | None] = models.IntegerField(
        null=True,
        blank=True,
    )
    patient_id: models.CharField[str, str] = models.CharField(max_length=255)
    patient_phone: models.CharField[str, str] = models.CharField(
        max_length=255,
        blank=True,
    )
    department_id: models.IntegerField[int | None, int | None] = models.IntegerField(
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
        return normalize_phone_number(self.patient_phone)

    def get_trackable_return_dates(self, today: date) -> list[date]:
        appointment_dates: list[date] = []

        for return_date in self.return_dates:
            if not isinstance(return_date, dict):
                continue

            appointment_date = _parse_return_date(return_date.get("return_date"))
            if (
                appointment_date is None
                or appointment_date + timedelta(weeks=8) < today
            ):
                continue

            appointment_dates.append(appointment_date)

        return appointment_dates

    def get_messaging_facility(
        self,
        *,
        facilities_by_id: dict[int, Facility] | None = None,
    ) -> Facility | None:
        if self.facility_id is None:
            return None

        if facilities_by_id is None:
            facility = Facility.objects.filter(
                ccmdd_facility_id=self.facility_id
            ).first()
        else:
            facility = facilities_by_id.get(self.facility_id)
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
