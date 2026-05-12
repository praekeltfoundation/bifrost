from __future__ import annotations

import logging
from datetime import datetime, timezone

from celery import shared_task
from django.conf import settings
from django.db import transaction
from django.db.models import Max
from django.utils import timezone as django_timezone

from lock.models import Lock, LockAcquisitionError
from synch.ccmdd import CCMDDAPIClient
from synch.models import Facility, Patient, Prescription
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
        with transaction.atomic():
            sync_patients(lock)
            sync_facilities(lock)
            sync_prescriptions(lock)
            sync_appointment_dates_to_turn(lock)
            sync_new_patients_to_turn(lock)
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


@shared_task
def sync_facilities(lock: Lock | None = None) -> None:
    client = _get_client()

    facilities: list[Facility] = []

    for record in client.iter_facilities():
        facility_id = record.pop("id")
        name = record.pop("level_desc_5")
        latitude = record.pop("latitude", None) or ""
        longitude = record.pop("longitude", None) or ""
        telephone = record.pop("telephone", None) or ""
        address_1 = record.pop("address_1", None) or ""
        address_2 = record.pop("address_2", None) or ""
        facilities.append(
            Facility(
                ccmdd_facility_id=facility_id,
                name=name,
                latitude=latitude,
                longitude=longitude,
                telephone=telephone,
                address_1=address_1,
                address_2=address_2,
                payload=record,
            )
        )

    if facilities:
        Facility.objects.bulk_create(
            facilities,
            update_conflicts=True,
            unique_fields=["ccmdd_facility_id"],
            update_fields=[
                "name",
                "latitude",
                "longitude",
                "telephone",
                "address_1",
                "address_2",
                "payload",
            ],
        )
        if lock is not None:
            lock.refresh()

    logger.info("Synced %s facilities.", len(facilities))


@shared_task
def sync_new_patients_to_turn(lock: Lock | None = None) -> None:
    new_patients = Patient.objects.filter(invite_sent=False)

    timestamp = django_timezone.now().isoformat()
    rows: list[dict[str, object]] = []
    updated_patient_ids: list[str] = []
    patient_urn_id_mapping: dict[str, str] = {}
    today = django_timezone.localdate()

    for patient in new_patients:
        sync_details = patient.get_turn_sync_details(today)

        if sync_details.messaging_phone_number is None:
            logger.info(
                "Patient %s does not have a messaging phone number, "
                "skipping Turn sync.",
                patient.ccmdd_patient_id,
            )
            continue

        if sync_details.upcoming_appointment is None:
            logger.info(
                "Patient %s does not have an upcoming appointment, skipping Turn sync.",
                patient.ccmdd_patient_id,
            )
            continue

        rows.append(
            {
                "urn": sync_details.messaging_phone_number,
                "synch_new_user": timestamp,
            }
        )
        patient_urn_id_mapping[sync_details.messaging_phone_number] = (
            patient.ccmdd_patient_id
        )
        updated_patient_ids.append(patient.ccmdd_patient_id)
        if lock is not None:
            lock.refresh()

    if not rows:
        logger.info("Imported 0 new patients to Turn.")
        return

    errors = _get_turn_client().import_contacts(rows)
    if errors:
        for error in errors:
            urn = error.get("urn") or ""
            patient_id = patient_urn_id_mapping.get(urn)
            if patient_id:
                updated_patient_ids.remove(patient_id)
        logger.error(
            "Turn returned import errors for %d contact row(s): %s",
            len(errors),
            repr(errors),
        )

    Patient.objects.filter(ccmdd_patient_id__in=updated_patient_ids).update(
        invite_sent=True
    )

    logger.info("Imported %s new patients to Turn.", len(rows))


@shared_task
def sync_appointment_dates_to_turn(lock: Lock | None = None) -> None:
    today = django_timezone.localdate()
    rows: list[dict[str, object]] = []

    for patient in Patient.objects.only("ccmdd_patient_id").iterator():
        sync_details = patient.get_turn_sync_details(today)
        if sync_details.messaging_phone_number is None:
            logger.info(
                "Patient %s does not have a messaging phone number, "
                "skipping Turn appointment sync.",
                patient.ccmdd_patient_id,
            )
            continue

        row: dict[str, object] = {
            "urn": sync_details.messaging_phone_number,
            "synch_next_appointment_date": "",
            "synch_appointment_facility_name": "",
            "synch_appointment_facility_latitude": "",
            "synch_appointment_facility_longitude": "",
        }

        if sync_details.upcoming_appointment is not None:
            appointment = sync_details.upcoming_appointment
            row["synch_next_appointment_date"] = appointment.date.isoformat()
            row["synch_appointment_facility_name"] = appointment.facility.name
            row["synch_appointment_facility_latitude"] = appointment.facility.latitude
            row["synch_appointment_facility_longitude"] = appointment.facility.longitude

        rows.append(row)
        if lock is not None:
            lock.refresh()

    if not rows:
        logger.info("Imported 0 appointment updates to Turn.")
        return

    errors = _get_turn_client().import_contacts(rows)
    if errors:
        raise TurnAPIError(
            f"Turn returned import errors for {len(errors)} contact row(s): {errors!r}"
        )

    logger.info("Imported %s appointment updates to Turn.", len(rows))
