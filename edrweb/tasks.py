from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from celery import shared_task
from django.conf import settings
from django.db import transaction
from django.db.models import Max
from django.utils import timezone as django_timezone

from edrweb.api import EDRWebAPIClient
from edrweb.models import EDRWebPatient
from lock.models import Lock, LockAcquisitionError

EDRWEB_APPOINTMENT_REMINDER_SYNC_LOCK_KEY = "sync-edrweb-appointment-reminders"
EDRWEB_APPOINTMENT_REMINDER_DELTA_LOCK_KEY = EDRWEB_APPOINTMENT_REMINDER_SYNC_LOCK_KEY
FULL_RECONCILIATION_LOCK_RETRY_SECONDS = 15 * 60
logger = logging.getLogger(__name__)


def _get_client() -> EDRWebAPIClient:
    return EDRWebAPIClient(
        base_url=settings.EDRWEB_BASE_URL,
        username=settings.EDRWEB_USERNAME,
        password=settings.EDRWEB_PASSWORD,
    )


def _is_api_configured() -> bool:
    return bool(
        settings.EDRWEB_BASE_URL
        and settings.EDRWEB_USERNAME
        and settings.EDRWEB_PASSWORD
    )


@shared_task
def sync_appointment_reminder_delta() -> None:
    if not _is_api_configured():
        logger.warning(
            "Skipping EDRWeb appointment reminder delta because EDRWEB_BASE_URL, "
            "EDRWEB_USERNAME, or EDRWEB_PASSWORD is not configured.",
        )
        return

    try:
        lock = Lock.acquire(key=EDRWEB_APPOINTMENT_REMINDER_DELTA_LOCK_KEY)
    except LockAcquisitionError:
        logger.warning(
            "Skipping EDRWeb appointment reminder delta because lock '%s' "
            "is already held.",
            EDRWEB_APPOINTMENT_REMINDER_DELTA_LOCK_KEY,
        )
        return

    try:
        synced = 0
        with transaction.atomic():
            latest_updated_at = EDRWebPatient.objects.aggregate(
                latest_updated_at=Max("updated_at")
            )["latest_updated_at"]
            updated_since = (
                latest_updated_at - timedelta(seconds=1)
                if latest_updated_at is not None
                else None
            )
            for record in _get_client().iter_appointment_reminder_records(
                updated_since=updated_since,
            ):
                if _upsert_appointment_reminder_record(record):
                    synced += 1
                lock.refresh()
    finally:
        lock.release()

    logger.info(
        "Synced EDRWeb appointment reminder records: %s.",
        synced,
    )


@shared_task(bind=True)
def sync_appointment_reminder_full_reconciliation(self: Any) -> None:
    if not _is_api_configured():
        logger.warning(
            "Skipping EDRWeb appointment reminder full reconciliation because "
            "EDRWEB_BASE_URL, EDRWEB_USERNAME, or EDRWEB_PASSWORD is not configured.",
        )
        return

    try:
        lock = Lock.acquire(key=EDRWEB_APPOINTMENT_REMINDER_SYNC_LOCK_KEY)
    except LockAcquisitionError as exc:
        logger.warning(
            "Retrying EDRWeb appointment reminder full reconciliation because lock "
            "'%s' is already held.",
            EDRWEB_APPOINTMENT_REMINDER_SYNC_LOCK_KEY,
        )
        raise self.retry(
            countdown=FULL_RECONCILIATION_LOCK_RETRY_SECONDS,
            max_retries=None,
        ) from exc

    try:
        synced_patient_ids: set[str] = set()
        with transaction.atomic():
            for record in _get_client().iter_appointment_reminder_records(
                updated_since=None,
            ):
                patient_id = record.get("PersonId")
                if isinstance(patient_id, str) and patient_id:
                    synced_patient_ids.add(patient_id)
                _upsert_appointment_reminder_record(record)
                lock.refresh()

            EDRWebPatient.objects.filter(is_active=True).exclude(
                patient_id__in=synced_patient_ids,
            ).update(
                is_active=False,
                feed_removed_at=django_timezone.now(),
            )
    finally:
        lock.release()

    logger.info(
        "Completed EDRWeb appointment reminder full reconciliation. "
        "Records synced: %s.",
        len(synced_patient_ids),
    )


def _upsert_appointment_reminder_record(record: dict[str, Any]) -> bool:
    payload = dict(record)
    patient_id = payload.pop("PersonId", None)
    if not isinstance(patient_id, str) or not patient_id:
        raise ValueError("EDRWeb PersonId field is required.")
    phone_number = payload.pop("PhoneNumber", "") or ""
    updated_at_value = payload.pop("UpdatedAt", None)
    if not isinstance(updated_at_value, str) or not updated_at_value:
        raise ValueError("EDRWeb UpdatedAt field is required.")
    updated_at = datetime.fromisoformat(updated_at_value)
    if updated_at.utcoffset() is None:
        raise ValueError("EDRWeb UpdatedAt field must include a timezone offset.")
    appointments = payload.pop("Appointments", [])
    if not isinstance(appointments, list):
        raise ValueError("EDRWeb Appointments field must be a list.")

    existing_patient = (
        EDRWebPatient.objects.select_for_update().filter(patient_id=patient_id).first()
    )
    if existing_patient is not None and updated_at < existing_patient.updated_at:
        if (
            not existing_patient.is_active
            or existing_patient.feed_removed_at is not None
        ):
            existing_patient.is_active = True
            existing_patient.feed_removed_at = None
            existing_patient.save(update_fields=["is_active", "feed_removed_at"])
            return True
        return False

    EDRWebPatient.objects.update_or_create(
        patient_id=patient_id,
        defaults={
            "phone_number": phone_number,
            "updated_at": updated_at,
            "is_active": True,
            "feed_removed_at": None,
            "appointments": appointments,
            "payload": payload,
        },
    )
    return True
