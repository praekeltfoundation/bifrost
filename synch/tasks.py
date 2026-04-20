from __future__ import annotations

import logging
from datetime import datetime, timezone

from celery import shared_task
from django.conf import settings
from django.db.models import Max

from lock.models import Lock, LockAcquisitionError
from synch.ccmdd import CCMDDAPIClient
from synch.models import Patient

PATIENT_SYNC_LOCK_KEY = "sync-patients"
PATIENT_SYNC_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
logger = logging.getLogger(__name__)


def _parse_ccmdd_timestamp(value: str) -> datetime:
    return datetime.strptime(value, PATIENT_SYNC_TIMESTAMP_FORMAT).replace(
        tzinfo=timezone.utc
    )


@shared_task
def healthcheck():
    return "OK"


@shared_task
def sync_patients() -> None:
    try:
        lock = Lock.acquire(key=PATIENT_SYNC_LOCK_KEY)
    except LockAcquisitionError:
        logger.warning(
            "Skipping patient sync because lock '%s' is already held.",
            PATIENT_SYNC_LOCK_KEY,
        )
        return

    try:
        latest_date_updated = Patient.objects.aggregate(
            latest_date_updated=Max("date_updated")
        )["latest_date_updated"]
        if latest_date_updated is None:
            latest_date_updated = EPOCH
        client = CCMDDAPIClient(
            base_url=settings.CCMDD_BASE_URL,
            username=settings.CCMDD_USERNAME,
            password=settings.CCMDD_PASSWORD,
        )

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
            lock.refresh()

        logger.info("Synced %s patients.", synced)
    finally:
        lock.release()
