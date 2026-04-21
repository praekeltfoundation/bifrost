from __future__ import annotations

import logging
from datetime import datetime, timezone

from celery import shared_task
from django.conf import settings
from django.db.models import Max

from lock.models import Lock, LockAcquisitionError
from synch.ccmdd import CCMDDAPIClient
from synch.models import Patient, Prescription

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
        sync_patients(lock)
        sync_prescriptions(lock)
    finally:
        lock.release()


@shared_task
def sync_patients(lock: Lock | None = None) -> None:
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
