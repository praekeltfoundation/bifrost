from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from celery import shared_task
from django.conf import settings
from django.db import transaction
from django.db.models import F, Max
from django.utils import timezone as django_timezone

from bifrost.phone_numbers import normalize_phone_number
from edrweb.api import EDRWebAPIClient
from edrweb.models import EDRWebPatient
from lock.models import Lock, LockAcquisitionError
from synch.turn import TurnAPIClient, TurnAPIError

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


def _get_turn_client() -> TurnAPIClient:
    return TurnAPIClient(
        base_url=settings.TURN_BASE_URL,
        token=settings.TURN_TOKEN,
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
        logger.info(
            "Synced EDRWeb appointment reminder records: %s.",
            synced,
        )
        sync_appointment_reminders_to_turn(lock)
        sync_messaging_contact_activations_to_turn(lock)
        sync_changed_patient_phone_numbers_to_turn(lock)
    finally:
        lock.release()


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
        logger.info(
            "Completed EDRWeb appointment reminder full reconciliation. "
            "Records synced: %s.",
            len(synced_patient_ids),
        )
        sync_appointment_reminders_to_turn(lock)
        sync_messaging_contact_activations_to_turn(lock)
        sync_changed_patient_phone_numbers_to_turn(lock)
    finally:
        lock.release()


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
    phone_number = normalize_phone_number(phone_number) or ""

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


def sync_appointment_reminders_to_turn(lock: Lock | None = None) -> None:
    rows: list[dict[str, object]] = []

    for patient in EDRWebPatient.objects.order_by("pk").iterator():
        row = patient.get_turn_sync_row()
        if row is None:
            logger.info(
                "EDRWeb Patient %s does not have a usable WhatsApp phone number, "
                "skipping Turn sync.",
                patient.patient_id,
            )
            continue

        rows.append(row)
        if lock is not None:
            lock.refresh()

    if not rows:
        logger.info("Imported 0 EDRWeb appointment reminder updates to Turn.")
        return

    errors = _get_turn_client().import_contacts(rows)
    if lock is not None:
        lock.refresh()
    if errors:
        raise TurnAPIError(
            "Turn returned import errors for "
            f"{len(errors)} EDRWeb appointment reminder row(s): {errors!r}"
        )

    logger.info(
        "Imported %s EDRWeb appointment reminder updates to Turn.",
        len(rows),
    )


def sync_messaging_contact_activations_to_turn(lock: Lock | None = None) -> None:
    timestamp = django_timezone.now().isoformat()
    rows: list[dict[str, object]] = []
    activation_attempts: list[tuple[str, str]] = []

    for patient in EDRWebPatient.objects.filter(
        is_active=True,
        messaging_contact_activated=False,
    ).order_by("pk"):
        row = patient.get_turn_activation_row(timestamp)
        if row is None:
            logger.info(
                "EDRWeb Patient %s does not have a usable WhatsApp phone number, "
                "skipping messaging contact activation.",
                patient.patient_id,
            )
            continue

        urn = str(row["urn"])
        rows.append(row)
        activation_attempts.append((patient.patient_id, urn))
        if lock is not None:
            lock.refresh()

    if not rows:
        logger.info("Imported 0 EDRWeb messaging contact activations to Turn.")
        return

    errors = _get_turn_client().import_contacts(rows)
    if lock is not None:
        lock.refresh()
    failed_urns: set[str] = set()
    if errors:
        failed_urns = {error.get("urn") or "" for error in errors}
        logger.error(
            "Turn returned import errors for %d EDRWeb messaging contact "
            "activation row(s): %s",
            len(errors),
            repr(errors),
        )

    updated_patient_ids = [
        patient_id for patient_id, urn in activation_attempts if urn not in failed_urns
    ]
    EDRWebPatient.objects.filter(patient_id__in=updated_patient_ids).update(
        messaging_contact_activated=True,
        active_messaging_phone_number=F("phone_number"),
    )

    logger.info(
        "Imported %s EDRWeb messaging contact activations to Turn.",
        len(rows),
    )


def sync_changed_patient_phone_numbers_to_turn(lock: Lock | None = None) -> None:
    timestamp = django_timezone.now().isoformat()
    patients = list(
        EDRWebPatient.objects.filter(
            is_active=True,
            messaging_contact_activated=True,
        )
        .exclude(active_messaging_phone_number="")
        .exclude(active_messaging_phone_number=F("phone_number"))
        .order_by("pk")
    )
    if lock is not None:
        lock.refresh()
    if not patients:
        logger.info("Imported 0 EDRWeb changed patient phone numbers to Turn.")
        return

    turn_client = _get_turn_client()
    retirement_errors = turn_client.import_contacts(
        [
            {
                "urn": patient.active_messaging_phone_number,
                "edrweb_reminders": "False",
            }
            for patient in patients
        ]
    )
    if lock is not None:
        lock.refresh()
    if retirement_errors:
        logger.error(
            "Turn returned retirement import errors for %d EDRWeb changed phone "
            "number row(s): %s",
            len(retirement_errors),
            repr(retirement_errors),
        )

    failed_retirement_urns = {error.get("urn") or "" for error in retirement_errors}
    activation_patients = [
        patient
        for patient in patients
        if patient.active_messaging_phone_number not in failed_retirement_urns
    ]
    if not activation_patients:
        logger.info("Imported 0 EDRWeb changed patient phone numbers to Turn.")
        return

    activation_errors = turn_client.import_contacts(
        [
            {
                "urn": patient.phone_number,
                "edrweb_new_user": timestamp,
            }
            for patient in activation_patients
        ]
    )
    if lock is not None:
        lock.refresh()
    if activation_errors:
        logger.error(
            "Turn returned activation import errors for %d EDRWeb changed phone "
            "number row(s): %s",
            len(activation_errors),
            repr(activation_errors),
        )

    failed_activation_urns = {error.get("urn") or "" for error in activation_errors}
    updated_patient_ids = [
        patient.patient_id
        for patient in activation_patients
        if patient.phone_number not in failed_activation_urns
    ]
    EDRWebPatient.objects.filter(
        patient_id__in=updated_patient_ids,
    ).update(
        active_messaging_phone_number=F("phone_number"),
    )

    logger.info(
        "Imported %s EDRWeb changed patient phone numbers to Turn.",
        len(updated_patient_ids),
    )
