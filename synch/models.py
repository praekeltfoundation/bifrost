from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
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


@dataclass(frozen=True)
class TrackedAppointment:
    date: date
    prescription: Prescription
    facility: Facility


@dataclass(frozen=True)
class PatientTurnSyncDetails:
    messaging_phone_number: str | None
    tracked_appointment: TrackedAppointment | None
    messaging_facility: Facility | None


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
            normalized_phone_number = normalize_phone_number(
                prescription.patient_phone,
            )
            if normalized_phone_number is not None:
                return normalized_phone_number

        return None

    def get_tracked_appointment(self, today: date) -> TrackedAppointment | None:
        prescriptions = list(self.prescriptions)
        facilities_by_id = self._get_facilities_by_id(prescriptions)
        return self._get_tracked_appointment_from_prescriptions(
            today,
            prescriptions,
            facilities_by_id,
        )

    def get_turn_sync_details(self, today: date) -> PatientTurnSyncDetails:
        prescriptions = list(self.prescriptions)
        facilities_by_id = self._get_facilities_by_id(prescriptions)
        tracked_appointment = self._get_tracked_appointment_from_prescriptions(
            today,
            prescriptions,
            facilities_by_id,
        )
        return PatientTurnSyncDetails(
            messaging_phone_number=self.messaging_phone_number,
            tracked_appointment=tracked_appointment,
            messaging_facility=self._get_messaging_facility_for_turn_sync(
                prescriptions,
                facilities_by_id,
                tracked_appointment,
            ),
        )

    def _get_facilities_by_id(
        self,
        prescriptions: list[Prescription],
    ) -> dict[int, Facility]:
        facility_ids = {
            prescription.facility_id
            for prescription in prescriptions
            if prescription.facility_id is not None
        }
        return {
            facility.ccmdd_facility_id: facility
            for facility in Facility.objects.filter(ccmdd_facility_id__in=facility_ids)
        }

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
