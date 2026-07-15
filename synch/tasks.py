from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

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


@dataclass(frozen=True)
class PatientPhoneNumberChange:
    patient: Patient
    old_phone_number: str
    new_phone_number: str


@dataclass(frozen=True)
class PatientMessagingSnapshot:
    patients: list[Patient]
    prescriptions_by_patient_id: dict[str, list[Prescription]]
    facilities_by_id: dict[int, Facility]
    today: date

    def get_turn_sync_details(self, patient: Patient):
        return patient.get_turn_sync_details(
            today=self.today,
            prescriptions=self.prescriptions_by_patient_id.get(
                patient.ccmdd_patient_id,
                [],
            ),
            facilities_by_id=self.facilities_by_id,
        )


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


def _get_turn_error_urns(errors: list[dict[str, str]]) -> set[str]:
    return {error.get("urn") or "" for error in errors}


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
            sync_facilities(lock)
            prescription_date_updated = Prescription.objects.aggregate(
                latest_date_updated=Max("date_updated")
            )["latest_date_updated"]
            if prescription_date_updated is None:
                prescription_date_updated = EPOCH
            sync_prescriptions(lock, date_updated=prescription_date_updated)
            sync_patients(lock, prescription_date_updated=prescription_date_updated)
            patient_messaging_snapshot = build_patient_messaging_snapshot()
            sync_appointment_dates_to_turn(patient_messaging_snapshot, lock)
            sync_new_patients_to_turn(patient_messaging_snapshot, lock)
            sync_changed_patient_phone_numbers_to_turn(patient_messaging_snapshot, lock)
    finally:
        lock.release()


def sync_patients(
    lock: Lock | None = None,
    *,
    prescription_date_updated: datetime,
) -> None:
    latest_date_updated = Patient.objects.aggregate(
        latest_date_updated=Max("date_updated")
    )["latest_date_updated"]
    if latest_date_updated is None:
        latest_date_updated = EPOCH
    client = _get_client()

    synced_by_patient_update = 0
    synced_by_prescription_update = 0

    for record in client.iter_limited_patients(date_updated=latest_date_updated):
        _upsert_patient_record(record)
        synced_by_patient_update += 1
        if lock is not None:
            lock.refresh()

    for record in client.iter_limited_patients(
        prescription_date_updated=prescription_date_updated
    ):
        _upsert_patient_record(record)
        synced_by_prescription_update += 1
        if lock is not None:
            lock.refresh()

    logger.info("Synced %s patients by patient update.", synced_by_patient_update)
    logger.info(
        "Synced %s patients by prescription update.",
        synced_by_prescription_update,
    )
    logger.info(
        "Synced %s patients total.",
        synced_by_patient_update + synced_by_prescription_update,
    )


def sync_prescriptions(
    lock: Lock | None = None,
    *,
    date_updated: datetime,
) -> None:
    client = _get_client()

    synced = 0

    for record in client.iter_limited_prescriptions(date_updated=date_updated):
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


def _upsert_patient_record(record: dict[str, Any]) -> None:
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


def build_patient_messaging_snapshot() -> PatientMessagingSnapshot:
    patients = list(Patient.objects.order_by("pk"))
    prescriptions_by_patient_id: dict[str, list[Prescription]] = defaultdict(list)
    facility_ids: set[int] = set()

    for prescription in Prescription.objects.order_by(
        "patient_id", "date_created", "pk"
    ):
        prescriptions_by_patient_id[prescription.patient_id].append(prescription)
        if prescription.facility_id is not None:
            facility_ids.add(prescription.facility_id)

    facilities_by_id = {
        facility.ccmdd_facility_id: facility
        for facility in Facility.objects.filter(ccmdd_facility_id__in=facility_ids)
    }

    return PatientMessagingSnapshot(
        patients=patients,
        prescriptions_by_patient_id=dict(prescriptions_by_patient_id),
        facilities_by_id=facilities_by_id,
        today=django_timezone.localdate(),
    )


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


def sync_new_patients_to_turn(
    patient_messaging_snapshot: PatientMessagingSnapshot,
    lock: Lock | None = None,
) -> None:
    timestamp = django_timezone.now().isoformat()
    rows: list[dict[str, object]] = []
    updated_patient_ids: list[str] = []
    patient_urn_id_mapping: dict[str, str] = {}
    patient_active_phone_numbers: dict[str, str] = {}

    for patient in patient_messaging_snapshot.patients:
        if patient.invite_sent:
            continue

        sync_details = patient_messaging_snapshot.get_turn_sync_details(patient)

        if sync_details.messaging_phone_number is None:
            logger.info(
                "Patient %s does not have a messaging phone number, "
                "skipping Turn sync.",
                patient.ccmdd_patient_id,
            )
            continue

        if sync_details.messaging_facility is None:
            logger.info(
                "Patient %s does not have a messaging facility, skipping Turn sync.",
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
        patient_active_phone_numbers[patient.ccmdd_patient_id] = (
            sync_details.messaging_phone_number
        )
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

    updated_patients = list(
        Patient.objects.filter(ccmdd_patient_id__in=updated_patient_ids)
    )
    for patient in updated_patients:
        patient.invite_sent = True
        patient.active_messaging_phone_number = patient_active_phone_numbers[
            patient.ccmdd_patient_id
        ]
    Patient.objects.bulk_update(
        updated_patients,
        ["invite_sent", "active_messaging_phone_number"],
    )

    logger.info("Imported %s new patients to Turn.", len(rows))


def sync_changed_patient_phone_numbers_to_turn(
    patient_messaging_snapshot: PatientMessagingSnapshot,
    lock: Lock | None = None,
) -> None:
    timestamp = django_timezone.now().isoformat()
    changes: list[PatientPhoneNumberChange] = []

    for patient in patient_messaging_snapshot.patients:
        if not patient.invite_sent or not patient.active_messaging_phone_number:
            continue

        sync_details = patient_messaging_snapshot.get_turn_sync_details(patient)
        messaging_phone_number = sync_details.messaging_phone_number
        if messaging_phone_number is None:
            logger.info(
                "Patient %s does not have a replacement messaging phone number, "
                "skipping changed phone number sync.",
                patient.ccmdd_patient_id,
            )
            continue

        if patient.active_messaging_phone_number == messaging_phone_number:
            continue

        changes.append(
            PatientPhoneNumberChange(
                patient=patient,
                old_phone_number=patient.active_messaging_phone_number,
                new_phone_number=messaging_phone_number,
            )
        )
        if lock is not None:
            lock.refresh()

    if not changes:
        logger.info("Imported 0 changed patient phone numbers to Turn.")
        return

    turn_client = _get_turn_client()
    retirement_errors = turn_client.import_contacts(
        [
            {
                "urn": change.old_phone_number,
                "synch_reminders": "False",
            }
            for change in changes
        ]
    )
    if retirement_errors:
        logger.error(
            "Turn returned retirement import errors for %d changed phone number "
            "row(s): %s",
            len(retirement_errors),
            repr(retirement_errors),
        )

    failed_retirement_urns = _get_turn_error_urns(retirement_errors)
    activation_changes = [
        change
        for change in changes
        if change.old_phone_number not in failed_retirement_urns
    ]
    if not activation_changes:
        logger.info("Imported 0 changed patient phone numbers to Turn.")
        return

    activation_errors = turn_client.import_contacts(
        [
            {
                "urn": change.new_phone_number,
                "synch_new_user": timestamp,
            }
            for change in activation_changes
        ]
    )
    if activation_errors:
        logger.error(
            "Turn returned activation import errors for %d changed phone number "
            "row(s): %s",
            len(activation_errors),
            repr(activation_errors),
        )

    failed_activation_urns = _get_turn_error_urns(activation_errors)
    updated_patients: list[Patient] = []
    for change in activation_changes:
        if change.new_phone_number in failed_activation_urns:
            continue

        change.patient.active_messaging_phone_number = change.new_phone_number
        updated_patients.append(change.patient)

    if updated_patients:
        Patient.objects.bulk_update(
            updated_patients,
            ["active_messaging_phone_number"],
        )

    logger.info(
        "Imported %s changed patient phone numbers to Turn.",
        len(updated_patients),
    )


def sync_appointment_dates_to_turn(
    patient_messaging_snapshot: PatientMessagingSnapshot,
    lock: Lock | None = None,
) -> None:
    rows: list[dict[str, object]] = []
    changed_patients: list[Patient] = []
    skipped_count = 0

    for patient in patient_messaging_snapshot.patients:
        sync_details = patient_messaging_snapshot.get_turn_sync_details(patient)
        if sync_details.messaging_phone_number is None:
            skipped_count += 1
            logger.info(
                "Patient %s does not have a messaging phone number, "
                "skipping Turn appointment sync.",
                patient.ccmdd_patient_id,
            )
            continue

        current_context = sync_details.get_turn_appointment_context_fields()
        if all(
            getattr(patient, field) == value for field, value in current_context.items()
        ):
            skipped_count += 1
            continue

        rows.append(sync_details.get_turn_appointment_import_row())
        for field, value in current_context.items():
            setattr(patient, field, value)
        changed_patients.append(patient)
        if lock is not None:
            lock.refresh()

    if not rows:
        logger.info(
            "Imported 0 appointment updates to Turn (0 changed, %s skipped).",
            skipped_count,
        )
        return

    errors = _get_turn_client().import_contacts(rows)
    if errors:
        raise TurnAPIError(
            f"Turn returned import errors for {len(errors)} contact row(s): {errors!r}"
        )

    synced_at = django_timezone.now()
    for patient in changed_patients:
        patient.turn_appointment_context_synced_at = synced_at
    Patient.objects.bulk_update(
        changed_patients,
        [*current_context.keys(), "turn_appointment_context_synced_at"],
    )

    logger.info(
        "Imported %s appointment updates to Turn (%s changed, %s skipped).",
        len(rows),
        len(rows),
        skipped_count,
    )
