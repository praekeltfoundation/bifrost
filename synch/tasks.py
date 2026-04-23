from __future__ import annotations

import logging
from datetime import date, datetime, timezone

import phonenumbers
from celery import shared_task
from django.conf import settings
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


def _get_next_appointment(
    prescriptions: list[Prescription],
    today: date,
) -> tuple[date, Prescription] | None:
    candidates: list[tuple[date, Prescription]] = []

    for prescription in prescriptions:
        for return_date in prescription.return_dates:
            if not isinstance(return_date, dict):
                continue
            appointment_date = _parse_return_date(return_date.get("return_date"))
            if appointment_date is None or appointment_date < today:
                continue
            candidates.append((appointment_date, prescription))

    if not candidates:
        return None

    return min(candidates, key=lambda candidate: candidate[0])


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
        sync_facilities(lock)
        sync_prescriptions(lock)
        sync_appointment_dates_to_turn(lock)
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


@shared_task
def sync_appointment_dates_to_turn(lock: Lock | None = None) -> None:
    today = django_timezone.localdate()
    rows: list[dict[str, object]] = []

    for patient in Patient.objects.only("ccmdd_patient_id").iterator():
        prescriptions = list(
            Prescription.objects.filter(patient_id=patient.ccmdd_patient_id).order_by(
                "date_created"
            )
        )
        if not prescriptions:
            logger.info(
                "No prescriptions found for patient %s, "
                "skipping Turn appointment sync.",
                patient.ccmdd_patient_id,
            )
            continue

        latest_prescription = prescriptions[-1]
        if not latest_prescription.patient_phone:
            logger.info(
                "Patient %s does not have a phone number, "
                "skipping Turn appointment sync.",
                patient.ccmdd_patient_id,
            )
            continue

        normalized_phone_number = _normalize_phone_number(
            latest_prescription.patient_phone
        )
        if normalized_phone_number is None:
            logger.info(
                "Patient %s has an unparseable phone number, "
                "skipping Turn appointment sync.",
                patient.ccmdd_patient_id,
            )
            continue

        next_appointment = _get_next_appointment(prescriptions, today)
        row: dict[str, object] = {
            "urn": normalized_phone_number,
            "synch_next_appointment_date": "",
            "synch_appointment_facility_name": "",
            "synch_appointment_facility_latitude": "",
            "synch_appointment_facility_longitude": "",
        }

        if next_appointment is not None:
            appointment_date, prescription = next_appointment
            facility = None
            if prescription.facility_id is not None:
                facility = Facility.objects.filter(
                    ccmdd_facility_id=prescription.facility_id
                ).first()

            row["synch_next_appointment_date"] = appointment_date.isoformat()
            if facility is not None:
                row["synch_appointment_facility_name"] = facility.name or ""
                row["synch_appointment_facility_latitude"] = facility.latitude or ""
                row["synch_appointment_facility_longitude"] = facility.longitude or ""

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
