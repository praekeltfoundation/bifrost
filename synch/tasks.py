from __future__ import annotations

import logging
from datetime import datetime, timezone

import phonenumbers
from celery import shared_task
from django.conf import settings
from django.db.models import Max
from django.utils import timezone as django_timezone

from lock.models import Lock, LockAcquisitionError
from synch.ccmdd import CCMDDAPIClient
from synch.models import Patient, Prescription
from synch.turn import TurnAPIClient, TurnAPIError

CCMDD_SYNC_LOCK_KEY = "sync-ccmdd"
CCMDD_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
logger = logging.getLogger(__name__)


def _parse_ccmdd_timestamp(value: str) -> datetime:
    return datetime.strptime(value, CCMDD_TIMESTAMP_FORMAT).replace(tzinfo=timezone.utc)


def _get_client() -> CCMDDAPIClient:
    return CCMDDAPIClient(
        base_url=settings.CCMDD_BASE_URL,
        username=settings.CCMDD_USERNAME,
        password=settings.CCMDD_PASSWORD,
    )


def _get_turn_client() -> TurnAPIClient:
    return TurnAPIClient(
        base_url=settings.TURN_BASE_URL,
        token=settings.TURN_TOKEN,
    )


def _normalize_phone_number(value: str) -> str | None:
    try:
        phone_number = phonenumbers.parse(value, "ZA")
    except phonenumbers.NumberParseException:
        return None

    return phonenumbers.format_number(
        phone_number,
        phonenumbers.PhoneNumberFormat.E164,
    )


@shared_task
def healthcheck():
    return "OK"


@shared_task
def sync_all() -> None:
    try:
        lock = Lock.acquire(key=CCMDD_SYNC_LOCK_KEY)
    except LockAcquisitionError:
        logger.warning(
            "Skipping CCMDD sync because lock '%s' is already held.",
            CCMDD_SYNC_LOCK_KEY,
        )
        return

    try:
        patient_sync_watermark = sync_patients(lock)
        sync_prescriptions(lock)
        sync_new_patients_to_turn(patient_sync_watermark, lock)
    finally:
        lock.release()


@shared_task
def sync_patients(lock: Lock | None = None) -> datetime:
    latest_date_updated = Patient.objects.aggregate(
        latest_date_updated=Max("date_updated")
    )["latest_date_updated"]
    if latest_date_updated is None:
        latest_date_updated = EPOCH
    client = _get_client()

    synced = 0

    for record in client.iter_limited_patients(date_updated=latest_date_updated):
        patient_id = record.pop("id")
        date_created = _parse_ccmdd_timestamp(record.pop("date_created"))
        date_updated = _parse_ccmdd_timestamp(record.pop("date_updated"))
        Patient.objects.update_or_create(
            ccmdd_patient_id=patient_id,
            defaults={
                "date_created": date_created,
                "date_updated": date_updated,
                "payload": record,
            },
        )
        synced += 1
        if lock is not None:
            lock.refresh()

    logger.info("Synced %s patients.", synced)
    return latest_date_updated


@shared_task
def sync_prescriptions(lock: Lock | None = None) -> None:
    latest_date_updated = Prescription.objects.aggregate(
        latest_date_updated=Max("date_updated")
    )["latest_date_updated"]
    if latest_date_updated is None:
        latest_date_updated = EPOCH
    client = _get_client()

    synced = 0

    for record in client.iter_limited_prescriptions(date_updated=latest_date_updated):
        prescription_id = record.pop("id")
        date_created = _parse_ccmdd_timestamp(record.pop("date_created"))
        date_updated = _parse_ccmdd_timestamp(record.pop("date_updated"))
        facility_id = record.pop("facility_id", None)
        patient_id = record.pop("patient_id")
        patient_phone = record.pop("patient_phone", "")
        department_id = record.pop("department_id", None)
        return_dates = record.pop("return_dates", [])
        Prescription.objects.update_or_create(
            ccmdd_prescription_id=prescription_id,
            defaults={
                "date_created": date_created,
                "date_updated": date_updated,
                "facility_id": facility_id,
                "patient_id": patient_id,
                "patient_phone": patient_phone,
                "department_id": department_id,
                "return_dates": return_dates,
                "payload": record,
            },
        )
        synced += 1
        if lock is not None:
            lock.refresh()

    logger.info("Synced %s prescriptions.", synced)


@shared_task
def sync_new_patients_to_turn(
    patient_sync_watermark: datetime,
    lock: Lock | None = None,
) -> None:
    new_patients = Patient.objects.filter(date_created__gt=patient_sync_watermark).only(
        "ccmdd_patient_id"
    )

    timestamp = django_timezone.now().isoformat()
    rows: list[dict[str, object]] = []

    for patient in new_patients:
        try:
            latest_prescription = Prescription.objects.filter(
                patient_id=patient.ccmdd_patient_id
            ).latest("date_created")
        except Prescription.DoesNotExist:
            logger.info(
                "No prescriptions found for patient %s, skipping Turn sync.",
                patient.ccmdd_patient_id,
            )
            continue

        if not latest_prescription.patient_phone:
            logger.info(
                "Patient %s does not have a phone number, skipping Turn sync",
                patient.ccmdd_patient_id,
            )
            continue

        normalized_phone_number = _normalize_phone_number(
            latest_prescription.patient_phone
        )
        if normalized_phone_number is None:
            logger.info(
                "Patient %s has an unparseable phone number, skipping Turn sync.",
                patient.ccmdd_patient_id,
            )
            continue

        rows.append(
            {
                "urn": normalized_phone_number,
                "synch_new_user": timestamp,
            }
        )
        if lock is not None:
            lock.refresh()

    if not rows:
        logger.info("Imported 0 new patients to Turn.")
        return

    errors = _get_turn_client().import_contacts(rows)
    if errors:
        raise TurnAPIError(
            f"Turn returned import errors for {len(errors)} contact row(s): {errors!r}"
        )

    logger.info("Imported %s new patients to Turn.", len(rows))
