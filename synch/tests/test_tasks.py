from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import Mock, patch

from celery.schedules import crontab
from django.test import TestCase, override_settings
from django.utils.module_loading import import_string

from bifrost.celery import app
from lock.models import Lock
from synch.models import Facility, Patient, Prescription
from synch.tasks import (
    healthcheck,
    sync_all,
    sync_appointment_dates_to_turn,
    sync_facilities,
    sync_new_patients_to_turn,
    sync_patients,
    sync_prescriptions,
)
from synch.turn import TurnAPIError

TEST_PASSWORD = "test-password"  # noqa: S105


class CeleryConfigurationTests(TestCase):
    def test_uses_amqp_broker_by_default(self):
        self.assertEqual(app.conf.broker_url, "amqp://guest:guest@localhost:5672//")

    def test_does_not_configure_result_backend(self):
        self.assertIsNone(app.conf.result_backend)

    def test_autodiscovers_shared_tasks_from_django_apps(self):
        task = import_string("synch.tasks.healthcheck")

        self.assertIn(task.name, app.tasks)
        self.assertEqual(app.tasks[task.name].name, task.name)

    def test_configures_five_minute_sync_schedule(self):
        self.assertEqual(
            app.conf.beat_schedule["sync-ccmdd"],
            {
                "task": "synch.tasks.sync_all",
                "schedule": crontab(minute="*/5"),
            },
        )


@override_settings(
    CELERY_TASK_ALWAYS_EAGER=True,
    CELERY_TASK_EAGER_PROPAGATES=True,
)
class CeleryTaskExecutionTests(TestCase):
    def test_healthcheck_task_runs(self):
        result = healthcheck.delay()

        self.assertTrue(result.successful())
        self.assertEqual(result.get(), "OK")

    def test_sync_all_runs_patient_sync_before_facility_sync_before_prescription_sync(
        self,
    ):
        with (
            patch(
                "synch.tasks.sync_patients",
                return_value=datetime(2026, 4, 1, tzinfo=timezone.utc),
            ) as sync_patients_mock,
            patch("synch.tasks.sync_facilities") as sync_facilities_mock,
            patch("synch.tasks.sync_prescriptions") as sync_prescriptions_mock,
            patch(
                "synch.tasks.sync_appointment_dates_to_turn"
            ) as sync_appointment_dates_to_turn_mock,
            patch("synch.tasks.sync_new_patients_to_turn") as sync_new_patients_to_turn,
        ):
            result = sync_all.delay()

        self.assertTrue(result.successful())
        sync_patients_mock.assert_called_once()
        sync_facilities_mock.assert_called_once()
        sync_prescriptions_mock.assert_called_once()
        sync_appointment_dates_to_turn_mock.assert_called_once_with(
            sync_patients_mock.call_args.args[0]
        )
        sync_new_patients_to_turn.assert_called_once_with(
            datetime(2026, 4, 1, tzinfo=timezone.utc),
            sync_patients_mock.call_args.args[0],
        )
        self.assertIs(
            sync_patients_mock.call_args.args[0],
            sync_facilities_mock.call_args.args[0],
        )
        self.assertIs(
            sync_patients_mock.call_args.args[0],
            sync_prescriptions_mock.call_args.args[0],
        )
        self.assertIs(
            sync_patients_mock.call_args.args[0],
            sync_appointment_dates_to_turn_mock.call_args.args[0],
        )

    def test_sync_all_does_not_run_facilities_or_prescriptions_when_patient_sync_fails(
        self,
    ):
        with (
            patch("synch.tasks.sync_patients", side_effect=RuntimeError("boom")),
            patch("synch.tasks.sync_facilities") as sync_facilities_mock,
            patch("synch.tasks.sync_prescriptions") as sync_prescriptions_mock,
            patch(
                "synch.tasks.sync_appointment_dates_to_turn"
            ) as sync_appointment_dates_to_turn_mock,
            patch("synch.tasks.sync_new_patients_to_turn") as sync_new_patients_to_turn,
            self.assertRaisesMessage(RuntimeError, "boom"),
        ):
            sync_all.delay()

        sync_facilities_mock.assert_not_called()
        sync_prescriptions_mock.assert_not_called()
        sync_appointment_dates_to_turn_mock.assert_not_called()
        sync_new_patients_to_turn.assert_not_called()

    def test_sync_all_does_not_run_prescriptions_when_facility_sync_fails(self):
        with (
            patch(
                "synch.tasks.sync_patients",
                return_value=datetime(2026, 4, 1, tzinfo=timezone.utc),
            ),
            patch("synch.tasks.sync_facilities", side_effect=RuntimeError("boom")),
            patch("synch.tasks.sync_prescriptions") as sync_prescriptions_mock,
            patch(
                "synch.tasks.sync_appointment_dates_to_turn"
            ) as sync_appointment_dates_to_turn_mock,
            patch("synch.tasks.sync_new_patients_to_turn") as sync_new_patients_to_turn,
            self.assertRaisesMessage(RuntimeError, "boom"),
        ):
            sync_all.delay()

        sync_prescriptions_mock.assert_not_called()
        sync_appointment_dates_to_turn_mock.assert_not_called()
        sync_new_patients_to_turn.assert_not_called()

    def test_sync_all_does_not_run_turn_tasks_when_prescription_sync_fails(self):
        with (
            patch(
                "synch.tasks.sync_patients",
                return_value=datetime(2026, 4, 1, tzinfo=timezone.utc),
            ),
            patch("synch.tasks.sync_facilities"),
            patch("synch.tasks.sync_prescriptions", side_effect=RuntimeError("boom")),
            patch(
                "synch.tasks.sync_appointment_dates_to_turn"
            ) as sync_appointment_dates_to_turn_mock,
            patch("synch.tasks.sync_new_patients_to_turn") as sync_new_patients_to_turn,
            self.assertRaisesMessage(RuntimeError, "boom"),
        ):
            sync_all.delay()

        sync_appointment_dates_to_turn_mock.assert_not_called()
        sync_new_patients_to_turn.assert_not_called()

    def test_sync_all_does_not_run_turn_sync_when_prescription_sync_fails(self):
        with (
            patch(
                "synch.tasks.sync_patients",
                return_value=datetime(2026, 4, 1, tzinfo=timezone.utc),
            ),
            patch("synch.tasks.sync_facilities"),
            patch("synch.tasks.sync_prescriptions", side_effect=RuntimeError("boom")),
            patch("synch.tasks.sync_new_patients_to_turn") as sync_new_patients_to_turn,
            self.assertRaisesMessage(RuntimeError, "boom"),
        ):
            sync_all.delay()

        sync_new_patients_to_turn.assert_not_called()

    def test_sync_all_propagates_turn_sync_errors(self):
        with (
            patch(
                "synch.tasks.sync_patients",
                return_value=datetime(2026, 4, 1, tzinfo=timezone.utc),
            ),
            patch("synch.tasks.sync_facilities"),
            patch("synch.tasks.sync_prescriptions"),
            patch(
                "synch.tasks.sync_new_patients_to_turn",
                side_effect=RuntimeError("boom"),
            ),
            self.assertRaisesMessage(RuntimeError, "boom"),
        ):
            sync_all.delay()

    def test_sync_all_rolls_back_database_updates_when_a_later_step_fails(self):
        def create_patient(lock: Lock) -> datetime:
            Patient.objects.create(
                ccmdd_patient_id="patient-1",
                date_created=datetime(2026, 4, 1, tzinfo=timezone.utc),
                date_updated=datetime(2026, 4, 1, tzinfo=timezone.utc),
                payload={},
            )
            return datetime(2026, 4, 1, tzinfo=timezone.utc)

        def create_facility(lock: Lock) -> None:
            Facility.objects.create(
                ccmdd_facility_id=1,
                name="Clinic",
                latitude="",
                longitude="",
                telephone="",
                address_1="",
                address_2="",
                payload={},
            )

        def create_prescription(lock: Lock) -> None:
            Prescription.objects.create(
                ccmdd_prescription_id="prescription-1",
                patient_id="patient-1",
                patient_phone="27820000000",
                facility_id=1,
                department_id=1,
                return_dates=[],
                date_created=datetime(2026, 4, 1, tzinfo=timezone.utc),
                date_updated=datetime(2026, 4, 1, tzinfo=timezone.utc),
                payload={},
            )

        with (
            patch("synch.tasks.sync_patients", side_effect=create_patient),
            patch("synch.tasks.sync_facilities", side_effect=create_facility),
            patch("synch.tasks.sync_prescriptions", side_effect=create_prescription),
            patch("synch.tasks.sync_appointment_dates_to_turn"),
            patch(
                "synch.tasks.sync_new_patients_to_turn",
                side_effect=RuntimeError("boom"),
            ),
            self.assertRaisesMessage(RuntimeError, "boom"),
        ):
            sync_all.delay()

        self.assertFalse(Patient.objects.exists())
        self.assertFalse(Facility.objects.exists())
        self.assertFalse(Prescription.objects.exists())

    def test_sync_all_skips_when_top_level_lock_is_already_held(self):
        Lock.acquire("sync-ccmdd")

        with (
            patch("synch.tasks.sync_patients") as sync_patients_mock,
            patch("synch.tasks.sync_facilities") as sync_facilities_mock,
            patch("synch.tasks.sync_prescriptions") as sync_prescriptions_mock,
            patch("synch.tasks.sync_new_patients_to_turn") as sync_new_patients_to_turn,
            self.assertLogs("synch.tasks", level="WARNING") as logs,
        ):
            result = sync_all.delay()

        self.assertTrue(result.successful())
        sync_patients_mock.assert_not_called()
        sync_facilities_mock.assert_not_called()
        sync_prescriptions_mock.assert_not_called()
        sync_new_patients_to_turn.assert_not_called()
        self.assertEqual(
            logs.output,
            [
                "WARNING:synch.tasks:"
                "Skipping CCMDD sync because lock 'sync-ccmdd' is already held."
            ],
        )


@override_settings(
    CELERY_TASK_ALWAYS_EAGER=True,
    CELERY_TASK_EAGER_PROPAGATES=True,
    CCMDD_BASE_URL="https://test.ccmdd.org.za",
    CCMDD_USERNAME="api-user",
    CCMDD_PASSWORD=TEST_PASSWORD,
)
class SyncPatientsTaskTests(TestCase):
    def test_sync_patients_uses_epoch_date_updated_when_no_patients_exist(self):
        client = Mock()
        client.iter_limited_patients.return_value = iter([])

        with patch("synch.tasks.CCMDDAPIClient", return_value=client):
            sync_patients.delay()

        client.iter_limited_patients.assert_called_once_with(
            date_updated=datetime(1970, 1, 1, tzinfo=timezone.utc),
        )

    def test_sync_patients_creates_records_and_strips_modeled_fields_from_payload(self):
        client = Mock()
        client.iter_limited_patients.return_value = iter(
            [
                {
                    "id": "90653BC3-DF69-E611-9D09-20689D5CEDFC",
                    "date_created": "2016-04-08 12:48:15.000",
                    "date_updated": "2016-04-29 11:25:28.000",
                    "surname": "wer",
                    "firstname": "wer",
                    "gender": "1",
                }
            ]
        )

        with (
            patch("synch.tasks.CCMDDAPIClient", return_value=client),
            self.assertLogs("synch.tasks", level="INFO") as logs,
        ):
            sync_patients.delay()

        patient = Patient.objects.get()
        self.assertEqual(
            patient.ccmdd_patient_id,
            "90653BC3-DF69-E611-9D09-20689D5CEDFC",
        )
        self.assertEqual(
            patient.payload,
            {"surname": "wer", "firstname": "wer", "gender": "1"},
        )
        client.iter_limited_patients.assert_called_once_with(
            date_updated=datetime(1970, 1, 1, tzinfo=timezone.utc),
        )
        self.assertEqual(logs.output, ["INFO:synch.tasks:Synced 1 patients."])

    def test_sync_patients_uses_latest_date_updated_for_incremental_sync(self):
        Patient.objects.create(
            ccmdd_patient_id="existing-patient",
            date_created=datetime(2016, 4, 8, 12, 48, 15, tzinfo=timezone.utc),
            date_updated=datetime(2016, 4, 29, 11, 25, 28, tzinfo=timezone.utc),
            payload={"surname": "old"},
        )
        client = Mock()
        client.iter_limited_patients.return_value = iter([])

        with patch("synch.tasks.CCMDDAPIClient", return_value=client):
            sync_patients.delay()

        client.iter_limited_patients.assert_called_once_with(
            date_updated=datetime(2016, 4, 29, 11, 25, 28, tzinfo=timezone.utc),
        )

    def test_sync_patients_updates_existing_patient_by_ccmdd_id(self):
        Patient.objects.create(
            ccmdd_patient_id="90653BC3-DF69-E611-9D09-20689D5CEDFC",
            date_created=datetime(2016, 4, 8, 12, 48, 15, tzinfo=timezone.utc),
            date_updated=datetime(2016, 4, 29, 11, 25, 28, tzinfo=timezone.utc),
            payload={"surname": "old"},
        )
        client = Mock()
        client.iter_limited_patients.return_value = iter(
            [
                {
                    "id": "90653BC3-DF69-E611-9D09-20689D5CEDFC",
                    "date_created": "2016-04-08 12:48:15.000",
                    "date_updated": "2016-04-30 09:00:00.000",
                    "surname": "new",
                    "firstname": "updated",
                }
            ]
        )

        with (
            patch("synch.tasks.CCMDDAPIClient", return_value=client),
            self.assertLogs("synch.tasks", level="INFO") as logs,
        ):
            sync_patients.delay()

        patient = Patient.objects.get(
            ccmdd_patient_id="90653BC3-DF69-E611-9D09-20689D5CEDFC"
        )
        self.assertEqual(patient.payload, {"surname": "new", "firstname": "updated"})
        self.assertEqual(
            patient.date_updated,
            datetime(2016, 4, 30, 9, 0, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(logs.output, ["INFO:synch.tasks:Synced 1 patients."])


@override_settings(
    CELERY_TASK_ALWAYS_EAGER=True,
    CELERY_TASK_EAGER_PROPAGATES=True,
    CCMDD_BASE_URL="https://test.ccmdd.org.za",
    CCMDD_USERNAME="api-user",
    CCMDD_PASSWORD=TEST_PASSWORD,
)
class SyncPrescriptionsTaskTests(TestCase):
    def test_sync_prescriptions_uses_epoch_date_updated_when_no_prescriptions_exist(
        self,
    ):
        client = Mock()
        client.iter_limited_prescriptions.return_value = iter([])

        with patch("synch.tasks.CCMDDAPIClient", return_value=client):
            sync_prescriptions.delay()

        client.iter_limited_prescriptions.assert_called_once_with(
            date_updated=datetime(1970, 1, 1, tzinfo=timezone.utc),
        )

    def test_sync_prescriptions_creates_records_and_strips_modeled_fields_from_payload(
        self,
    ):
        client = Mock()
        client.iter_limited_prescriptions.return_value = iter(
            [
                {
                    "id": "B2798F40-FA2C-F111-AD54-010101010000",
                    "date_created": "2026-03-31 14:07:57.167",
                    "date_updated": "2026-03-31 14:07:57.433",
                    "facility_id": 937324,
                    "patient_id": "D905C1E4-1962-E711-9D8C-7C5CF8BA146D",
                    "patient_phone": "1231231233",
                    "department_id": 123,
                    "return_dates": [
                        {
                            "return_date": "2026-04-28",
                            "note": "Testing",
                            "description": "1 month",
                            "day_count": 28,
                        }
                    ],
                    "prescription_status_id": 5,
                    "status_description": "Submitted",
                }
            ]
        )

        with (
            patch("synch.tasks.CCMDDAPIClient", return_value=client),
            self.assertLogs("synch.tasks", level="INFO") as logs,
        ):
            sync_prescriptions.delay()

        prescription = Prescription.objects.get()
        self.assertEqual(
            prescription.ccmdd_prescription_id,
            "B2798F40-FA2C-F111-AD54-010101010000",
        )
        self.assertEqual(prescription.facility_id, 937324)
        self.assertEqual(
            prescription.patient_id,
            "D905C1E4-1962-E711-9D8C-7C5CF8BA146D",
        )
        self.assertEqual(prescription.patient_phone, "1231231233")
        self.assertEqual(prescription.department_id, 123)
        self.assertEqual(
            prescription.return_dates,
            [
                {
                    "return_date": "2026-04-28",
                    "note": "Testing",
                    "description": "1 month",
                    "day_count": 28,
                }
            ],
        )
        self.assertEqual(
            prescription.payload,
            {"prescription_status_id": 5, "status_description": "Submitted"},
        )
        client.iter_limited_prescriptions.assert_called_once_with(
            date_updated=datetime(1970, 1, 1, tzinfo=timezone.utc),
        )
        self.assertEqual(logs.output, ["INFO:synch.tasks:Synced 1 prescriptions."])

    def test_sync_prescriptions_uses_latest_date_updated_for_incremental_sync(self):
        Prescription.objects.create(
            ccmdd_prescription_id="existing-prescription",
            date_created=datetime(2026, 3, 31, 14, 7, 57, 167000, tzinfo=timezone.utc),
            date_updated=datetime(2026, 3, 31, 14, 7, 57, 433000, tzinfo=timezone.utc),
            facility_id=937324,
            patient_id="existing-patient",
            patient_phone="1231231233",
            department_id=123,
            return_dates=[],
            payload={"status_description": "Submitted"},
        )
        client = Mock()
        client.iter_limited_prescriptions.return_value = iter([])

        with patch("synch.tasks.CCMDDAPIClient", return_value=client):
            sync_prescriptions.delay()

        client.iter_limited_prescriptions.assert_called_once_with(
            date_updated=datetime(2026, 3, 31, 14, 7, 57, 433000, tzinfo=timezone.utc),
        )

    def test_sync_prescriptions_updates_existing_prescription_by_ccmdd_id(self):
        Prescription.objects.create(
            ccmdd_prescription_id="B2798F40-FA2C-F111-AD54-010101010000",
            date_created=datetime(2026, 3, 31, 14, 7, 57, 167000, tzinfo=timezone.utc),
            date_updated=datetime(2026, 3, 31, 14, 7, 57, 433000, tzinfo=timezone.utc),
            facility_id=937324,
            patient_id="existing-patient",
            patient_phone="1231231233",
            department_id=123,
            return_dates=[],
            payload={"status_description": "Submitted"},
        )
        client = Mock()
        client.iter_limited_prescriptions.return_value = iter(
            [
                {
                    "id": "B2798F40-FA2C-F111-AD54-010101010000",
                    "date_created": "2026-03-31 14:07:57.167",
                    "date_updated": "2026-04-01 09:00:00.000",
                    "facility_id": 937325,
                    "patient_id": "updated-patient",
                    "patient_phone": "9998887777",
                    "department_id": 456,
                    "return_dates": [
                        {
                            "return_date": "2026-09-17",
                            "note": "PrEP",
                            "description": "6 Months",
                            "day_count": 168,
                        }
                    ],
                    "status_description": "Updated",
                }
            ]
        )

        with (
            patch("synch.tasks.CCMDDAPIClient", return_value=client),
            self.assertLogs("synch.tasks", level="INFO") as logs,
        ):
            sync_prescriptions.delay()

        prescription = Prescription.objects.get(
            ccmdd_prescription_id="B2798F40-FA2C-F111-AD54-010101010000"
        )
        self.assertEqual(prescription.facility_id, 937325)
        self.assertEqual(prescription.patient_id, "updated-patient")
        self.assertEqual(prescription.patient_phone, "9998887777")
        self.assertEqual(prescription.department_id, 456)
        self.assertEqual(
            prescription.return_dates,
            [
                {
                    "return_date": "2026-09-17",
                    "note": "PrEP",
                    "description": "6 Months",
                    "day_count": 168,
                }
            ],
        )
        self.assertEqual(prescription.payload, {"status_description": "Updated"})
        self.assertEqual(
            prescription.date_updated,
            datetime(2026, 4, 1, 9, 0, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(logs.output, ["INFO:synch.tasks:Synced 1 prescriptions."])


@override_settings(
    CELERY_TASK_ALWAYS_EAGER=True,
    CELERY_TASK_EAGER_PROPAGATES=True,
    CCMDD_BASE_URL="https://test.ccmdd.org.za",
    CCMDD_USERNAME="api-user",
    CCMDD_PASSWORD=TEST_PASSWORD,
)
class SyncFacilitiesTaskTests(TestCase):
    def test_sync_facilities_creates_records_and_strips_modeled_fields_from_payload(
        self,
    ):
        client = Mock()
        client.iter_facilities.return_value = iter(
            [
                {
                    "id": 110533,
                    "level_desc_5": "Addo Clinic",
                    "latitude": "-33.5422",
                    "longitude": "25.6908",
                    "telephone": "0421234567",
                    "address_1": "Main Road",
                    "address_2": "Addo",
                    "classification": "Clinic",
                    "active": 1,
                }
            ]
        )

        with (
            patch("synch.tasks.CCMDDAPIClient", return_value=client),
            self.assertLogs("synch.tasks", level="INFO") as logs,
        ):
            sync_facilities.delay()

        facility = Facility.objects.get()
        self.assertEqual(facility.ccmdd_facility_id, 110533)
        self.assertEqual(facility.name, "Addo Clinic")
        self.assertEqual(facility.latitude, "-33.5422")
        self.assertEqual(facility.longitude, "25.6908")
        self.assertEqual(facility.telephone, "0421234567")
        self.assertEqual(facility.address_1, "Main Road")
        self.assertEqual(facility.address_2, "Addo")
        self.assertEqual(facility.payload, {"classification": "Clinic", "active": 1})
        client.iter_facilities.assert_called_once_with()
        self.assertEqual(logs.output, ["INFO:synch.tasks:Synced 1 facilities."])

    def test_sync_facilities_updates_existing_facility_by_ccmdd_id(self):
        Facility.objects.create(
            ccmdd_facility_id=110533,
            name="Old Addo Clinic",
            latitude="-33.5000",
            longitude="25.6000",
            telephone="0000000000",
            address_1="Old Address",
            address_2="Old Suburb",
            payload={"classification": "Old"},
        )
        client = Mock()
        client.iter_facilities.return_value = iter(
            [
                {
                    "id": 110533,
                    "level_desc_5": "Addo Clinic",
                    "latitude": "-33.5422",
                    "longitude": "25.6908",
                    "telephone": "0421234567",
                    "address_1": "Main Road",
                    "address_2": "Addo",
                    "classification": "Clinic",
                }
            ]
        )

        with (
            patch("synch.tasks.CCMDDAPIClient", return_value=client),
            self.assertLogs("synch.tasks", level="INFO") as logs,
        ):
            sync_facilities.delay()

        facility = Facility.objects.get(ccmdd_facility_id=110533)
        self.assertEqual(facility.name, "Addo Clinic")
        self.assertEqual(facility.latitude, "-33.5422")
        self.assertEqual(facility.longitude, "25.6908")
        self.assertEqual(facility.telephone, "0421234567")
        self.assertEqual(facility.address_1, "Main Road")
        self.assertEqual(facility.address_2, "Addo")
        self.assertEqual(facility.payload, {"classification": "Clinic"})
        self.assertEqual(logs.output, ["INFO:synch.tasks:Synced 1 facilities."])

    def test_sync_facilities_bulk_upserts_existing_and_new_facilities(self):
        Facility.objects.create(
            ccmdd_facility_id=110533,
            name="Old Addo Clinic",
            latitude="-33.5000",
            longitude="25.6000",
            telephone="0000000000",
            address_1="Old Address",
            address_2="Old Suburb",
            payload={"classification": "Old"},
        )
        client = Mock()
        client.iter_facilities.return_value = iter(
            [
                {
                    "id": 110533,
                    "level_desc_5": "Addo Clinic",
                    "latitude": "-33.5422",
                    "longitude": "25.6908",
                    "telephone": "0421234567",
                    "address_1": "Main Road",
                    "address_2": "Addo",
                    "classification": "Clinic",
                },
                {
                    "id": 220044,
                    "level_desc_5": "New Town Clinic",
                    "latitude": "-34.0000",
                    "longitude": "26.0000",
                    "telephone": "0410000000",
                    "address_1": "1 New Street",
                    "address_2": "New Town",
                    "classification": "Satellite",
                    "active": 1,
                },
            ]
        )

        with (
            patch("synch.tasks.CCMDDAPIClient", return_value=client),
            self.assertLogs("synch.tasks", level="INFO") as logs,
        ):
            sync_facilities.delay()

        updated_facility = Facility.objects.get(ccmdd_facility_id=110533)
        self.assertEqual(updated_facility.name, "Addo Clinic")
        self.assertEqual(updated_facility.latitude, "-33.5422")
        self.assertEqual(updated_facility.longitude, "25.6908")
        self.assertEqual(updated_facility.telephone, "0421234567")
        self.assertEqual(updated_facility.address_1, "Main Road")
        self.assertEqual(updated_facility.address_2, "Addo")
        self.assertEqual(updated_facility.payload, {"classification": "Clinic"})

        created_facility = Facility.objects.get(ccmdd_facility_id=220044)
        self.assertEqual(created_facility.name, "New Town Clinic")
        self.assertEqual(created_facility.latitude, "-34.0000")
        self.assertEqual(created_facility.longitude, "26.0000")
        self.assertEqual(created_facility.telephone, "0410000000")
        self.assertEqual(created_facility.address_1, "1 New Street")
        self.assertEqual(created_facility.address_2, "New Town")
        self.assertEqual(
            created_facility.payload,
            {"classification": "Satellite", "active": 1},
        )
        self.assertEqual(Facility.objects.count(), 2)
        self.assertEqual(logs.output, ["INFO:synch.tasks:Synced 2 facilities."])


@override_settings(
    CELERY_TASK_ALWAYS_EAGER=True,
    CELERY_TASK_EAGER_PROPAGATES=True,
    TURN_BASE_URL="https://whatsapp.turn.io",
    TURN_TOKEN=TEST_PASSWORD,
)
class SyncNewPatientsToTurnTests(TestCase):
    def test_sync_new_patients_to_turn_imports_latest_prescription_phone(
        self,
    ):
        Patient.objects.create(
            ccmdd_patient_id="existing-patient",
            date_created=datetime(2026, 3, 31, 23, 59, 59, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 1, 0, 0, 0, tzinfo=timezone.utc),
            payload={},
        )
        new_patient = Patient.objects.create(
            ccmdd_patient_id="new-patient",
            date_created=datetime(2026, 4, 1, 0, 0, 1, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 1, 0, 5, 0, tzinfo=timezone.utc),
            payload={},
        )
        Prescription.objects.create(
            ccmdd_prescription_id="old-rx",
            date_created=datetime(2026, 4, 1, 1, 0, 0, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 1, 1, 0, 0, tzinfo=timezone.utc),
            facility_id=1,
            patient_id=new_patient.ccmdd_patient_id,
            patient_phone="0820000001",
            department_id=1,
            return_dates=[],
            payload={},
        )
        Prescription.objects.create(
            ccmdd_prescription_id="new-rx",
            date_created=datetime(2026, 4, 2, 1, 0, 0, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 2, 1, 0, 0, tzinfo=timezone.utc),
            facility_id=1,
            patient_id=new_patient.ccmdd_patient_id,
            patient_phone="0820000002",
            department_id=1,
            return_dates=[],
            payload={},
        )
        Prescription.objects.create(
            ccmdd_prescription_id="existing-rx",
            date_created=datetime(2026, 4, 2, 2, 0, 0, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 2, 2, 0, 0, tzinfo=timezone.utc),
            facility_id=1,
            patient_id="existing-patient",
            patient_phone="0820000003",
            department_id=1,
            return_dates=[],
            payload={},
        )
        turn_client = Mock()
        turn_client.import_contacts.return_value = []

        with (
            patch("synch.tasks.TurnAPIClient", return_value=turn_client),
            patch(
                "synch.tasks.django_timezone.now",
                return_value=datetime(2026, 4, 21, 10, 11, 12, tzinfo=timezone.utc),
            ),
            self.assertLogs("synch.tasks", level="INFO") as logs,
        ):
            sync_new_patients_to_turn(
                datetime(2026, 4, 1, 0, 0, 0, tzinfo=timezone.utc)
            )

        turn_client.import_contacts.assert_called_once_with(
            [
                {
                    "urn": "+27820000002",
                    "synch_new_user": "2026-04-21T10:11:12+00:00",
                }
            ]
        )
        self.assertEqual(
            logs.output, ["INFO:synch.tasks:Imported 1 new patients to Turn."]
        )

    def test_sync_new_patients_to_turn_normalizes_phone_to_e164_for_south_africa(
        self,
    ):
        patient = Patient.objects.create(
            ccmdd_patient_id="new-patient",
            date_created=datetime(2026, 4, 1, 0, 0, 1, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 1, 0, 0, 1, tzinfo=timezone.utc),
            payload={},
        )
        Prescription.objects.create(
            ccmdd_prescription_id="rx",
            date_created=datetime(2026, 4, 2, 1, 0, 0, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 2, 1, 0, 0, tzinfo=timezone.utc),
            facility_id=1,
            patient_id=patient.ccmdd_patient_id,
            patient_phone="082 123 4567",
            department_id=1,
            return_dates=[],
            payload={},
        )
        turn_client = Mock()
        turn_client.import_contacts.return_value = []

        with (
            patch("synch.tasks.TurnAPIClient", return_value=turn_client),
            patch(
                "synch.tasks.django_timezone.now",
                return_value=datetime(2026, 4, 21, 10, 11, 12, tzinfo=timezone.utc),
            ),
        ):
            sync_new_patients_to_turn(
                datetime(2026, 4, 1, 0, 0, 0, tzinfo=timezone.utc)
            )
        turn_client.import_contacts.assert_called_once_with(
            [
                {
                    "urn": "+27821234567",
                    "synch_new_user": "2026-04-21T10:11:12+00:00",
                }
            ]
        )

        turn_client.import_contacts.assert_called_once_with(
            [
                {
                    "urn": "+27821234567",
                    "synch_new_user": "2026-04-21T10:11:12+00:00",
                }
            ]
        )

    def test_sync_new_patients_to_turn_skips_patients_with_unparseable_phone_numbers(
        self,
    ):
        patient = Patient.objects.create(
            ccmdd_patient_id="new-patient",
            date_created=datetime(2026, 4, 1, 0, 0, 1, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 1, 0, 0, 1, tzinfo=timezone.utc),
            payload={},
        )
        Prescription.objects.create(
            ccmdd_prescription_id="rx",
            date_created=datetime(2026, 4, 2, 1, 0, 0, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 2, 1, 0, 0, tzinfo=timezone.utc),
            facility_id=1,
            patient_id=patient.ccmdd_patient_id,
            patient_phone="not-a-phone-number",
            department_id=1,
            return_dates=[],
            payload={},
        )
        turn_client = Mock()
        turn_client.import_contacts.return_value = []

        with (
            patch("synch.tasks.TurnAPIClient", return_value=turn_client),
            self.assertLogs("synch.tasks", level="INFO") as logs,
        ):
            sync_new_patients_to_turn(
                datetime(2026, 4, 1, 0, 0, 0, tzinfo=timezone.utc)
            )

        turn_client.import_contacts.assert_not_called()
        self.assertEqual(
            logs.output,
            [
                "INFO:synch.tasks:Patient new-patient has an unparseable phone "
                "number, skipping Turn sync.",
                "INFO:synch.tasks:Imported 0 new patients to Turn.",
            ],
        )

    def test_sync_new_patients_to_turn_skips_patients_without_prescriptions_or_phone(
        self,
    ):
        Patient.objects.create(
            ccmdd_patient_id="no-prescription",
            date_created=datetime(2026, 4, 1, 0, 0, 1, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 1, 0, 0, 1, tzinfo=timezone.utc),
            payload={},
        )
        blank_phone_patient = Patient.objects.create(
            ccmdd_patient_id="blank-phone",
            date_created=datetime(2026, 4, 1, 0, 0, 2, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 1, 0, 0, 2, tzinfo=timezone.utc),
            payload={},
        )
        Prescription.objects.create(
            ccmdd_prescription_id="blank-phone-rx",
            date_created=datetime(2026, 4, 2, 1, 0, 0, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 2, 1, 0, 0, tzinfo=timezone.utc),
            facility_id=1,
            patient_id=blank_phone_patient.ccmdd_patient_id,
            patient_phone="",
            department_id=1,
            return_dates=[],
            payload={},
        )
        turn_client = Mock()

        with patch("synch.tasks.TurnAPIClient", return_value=turn_client):
            sync_new_patients_to_turn(
                datetime(2026, 4, 1, 0, 0, 0, tzinfo=timezone.utc)
            )

        turn_client.import_contacts.assert_not_called()

    def test_sync_new_patients_to_turn_skips_turn_when_no_new_patients(
        self,
    ):
        Patient.objects.create(
            ccmdd_patient_id="existing-patient",
            date_created=datetime(2026, 4, 1, 0, 0, 0, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 1, 0, 0, 0, tzinfo=timezone.utc),
            payload={},
        )
        turn_client = Mock()

        with patch("synch.tasks.TurnAPIClient", return_value=turn_client):
            sync_new_patients_to_turn(
                datetime(2026, 4, 1, 0, 0, 0, tzinfo=timezone.utc)
            )

        turn_client.import_contacts.assert_not_called()

    def test_sync_new_patients_to_turn_propagates_turn_import_errors(self):
        patient = Patient.objects.create(
            ccmdd_patient_id="new-patient",
            date_created=datetime(2026, 4, 1, 0, 0, 1, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 1, 0, 0, 1, tzinfo=timezone.utc),
            payload={},
        )
        Prescription.objects.create(
            ccmdd_prescription_id="rx",
            date_created=datetime(2026, 4, 2, 1, 0, 0, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 2, 1, 0, 0, tzinfo=timezone.utc),
            facility_id=1,
            patient_id=patient.ccmdd_patient_id,
            patient_phone="0820000002",
            department_id=1,
            return_dates=[],
            payload={},
        )
        turn_client = Mock()
        turn_client.import_contacts.side_effect = RuntimeError("boom")

        with (
            patch("synch.tasks.TurnAPIClient", return_value=turn_client),
            patch(
                "synch.tasks.django_timezone.now",
                return_value=datetime(2026, 4, 21, 10, 11, 12, tzinfo=timezone.utc),
            ),
            self.assertRaisesMessage(RuntimeError, "boom"),
        ):
            sync_new_patients_to_turn(
                datetime(2026, 4, 1, 0, 0, 0, tzinfo=timezone.utc)
            )

    def test_sync_new_patients_to_turn_raises_when_turn_returns_row_errors(self):
        patient = Patient.objects.create(
            ccmdd_patient_id="new-patient",
            date_created=datetime(2026, 4, 1, 0, 0, 1, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 1, 0, 0, 1, tzinfo=timezone.utc),
            payload={},
        )
        Prescription.objects.create(
            ccmdd_prescription_id="rx",
            date_created=datetime(2026, 4, 2, 1, 0, 0, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 2, 1, 0, 0, tzinfo=timezone.utc),
            facility_id=1,
            patient_id=patient.ccmdd_patient_id,
            patient_phone="0820000002",
            department_id=1,
            return_dates=[],
            payload={},
        )
        turn_client = Mock()
        turn_client.import_contacts.return_value = [
            {
                "urn": "+27820000002",
                "synch_new_user": "2026-04-21T10:11:12+00:00",
                "error": "ERROR: duplicate contact",
            }
        ]

        with (
            patch("synch.tasks.TurnAPIClient", return_value=turn_client),
            patch(
                "synch.tasks.django_timezone.now",
                return_value=datetime(2026, 4, 21, 10, 11, 12, tzinfo=timezone.utc),
            ),
            self.assertRaisesMessage(
                TurnAPIError,
                "Turn returned import errors for 1 contact row(s): "
                "[{'urn': '+27820000002', 'synch_new_user': "
                "'2026-04-21T10:11:12+00:00', 'error': "
                "'ERROR: duplicate contact'}]",
            ),
        ):
            sync_new_patients_to_turn(
                datetime(2026, 4, 1, 0, 0, 0, tzinfo=timezone.utc)
            )


@override_settings(
    CELERY_TASK_ALWAYS_EAGER=True,
    CELERY_TASK_EAGER_PROPAGATES=True,
    TURN_BASE_URL="https://turn.io",
    TURN_TOKEN=TEST_PASSWORD,
)
class SyncAppointmentDatesToTurnTests(TestCase):
    def test_sync_appointment_dates_to_turn_imports_next_future_appointment(
        self,
    ):
        patient = Patient.objects.create(
            ccmdd_patient_id="patient-with-appointment",
            date_created=datetime(2026, 4, 1, 0, 0, 1, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 1, 0, 0, 1, tzinfo=timezone.utc),
            payload={},
        )
        Facility.objects.create(
            ccmdd_facility_id=123,
            name="Clinic A",
            latitude="-26.2041",
            longitude="28.0473",
            telephone="",
            address_1="",
            address_2="",
            payload={},
        )
        Prescription.objects.create(
            ccmdd_prescription_id="rx-old-phone",
            date_created=datetime(2026, 4, 2, 1, 0, 0, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 2, 1, 0, 0, tzinfo=timezone.utc),
            facility_id=123,
            patient_id=patient.ccmdd_patient_id,
            patient_phone="0820000001",
            department_id=1,
            return_dates=[
                {"return_date": "2026-04-30"},
                {"return_date": "2026-04-21"},
            ],
            payload={},
        )
        Prescription.objects.create(
            ccmdd_prescription_id="rx-new-phone",
            date_created=datetime(2026, 4, 3, 1, 0, 0, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 3, 1, 0, 0, tzinfo=timezone.utc),
            facility_id=999,
            patient_id=patient.ccmdd_patient_id,
            patient_phone="0820000002",
            department_id=1,
            return_dates=[
                {"return_date": "2026-05-05"},
                {"return_date": "2026-04-22"},
            ],
            payload={},
        )
        turn_client = Mock()
        turn_client.import_contacts.return_value = []

        with (
            patch("synch.tasks.TurnAPIClient", return_value=turn_client),
            patch(
                "synch.tasks.django_timezone.localdate",
                return_value=datetime(2026, 4, 21).date(),
            ),
            self.assertLogs("synch.tasks", level="INFO") as logs,
        ):
            sync_appointment_dates_to_turn()

        turn_client.import_contacts.assert_called_once_with(
            [
                {
                    "urn": "+27820000002",
                    "synch_next_appointment_date": "2026-04-21",
                    "synch_appointment_facility_name": "Clinic A",
                    "synch_appointment_facility_latitude": "-26.2041",
                    "synch_appointment_facility_longitude": "28.0473",
                }
            ]
        )
        self.assertEqual(
            logs.output, ["INFO:synch.tasks:Imported 1 appointment updates to Turn."]
        )

    def test_sync_appointment_dates_to_turn_clears_fields_when_no_upcoming_appointment(
        self,
    ):
        patient = Patient.objects.create(
            ccmdd_patient_id="patient-no-upcoming-appointment",
            date_created=datetime(2026, 4, 1, 0, 0, 1, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 1, 0, 0, 1, tzinfo=timezone.utc),
            payload={},
        )
        Prescription.objects.create(
            ccmdd_prescription_id="rx-past-only",
            date_created=datetime(2026, 4, 2, 1, 0, 0, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 2, 1, 0, 0, tzinfo=timezone.utc),
            facility_id=123,
            patient_id=patient.ccmdd_patient_id,
            patient_phone="0820000002",
            department_id=1,
            return_dates=[
                {"return_date": "2026-04-20"},
                {"return_date": "2026-04-01"},
            ],
            payload={},
        )
        turn_client = Mock()
        turn_client.import_contacts.return_value = []

        with (
            patch("synch.tasks.TurnAPIClient", return_value=turn_client),
            patch(
                "synch.tasks.django_timezone.localdate",
                return_value=datetime(2026, 4, 21).date(),
            ),
        ):
            sync_appointment_dates_to_turn()

        turn_client.import_contacts.assert_called_once_with(
            [
                {
                    "urn": "+27820000002",
                    "synch_next_appointment_date": "",
                    "synch_appointment_facility_name": "",
                    "synch_appointment_facility_latitude": "",
                    "synch_appointment_facility_longitude": "",
                }
            ]
        )

    def test_sync_appointment_dates_to_turn_skips_unparseable_phone_numbers(
        self,
    ):
        patient = Patient.objects.create(
            ccmdd_patient_id="patient-bad-phone",
            date_created=datetime(2026, 4, 1, 0, 0, 1, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 1, 0, 0, 1, tzinfo=timezone.utc),
            payload={},
        )
        Prescription.objects.create(
            ccmdd_prescription_id="rx",
            date_created=datetime(2026, 4, 2, 1, 0, 0, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 2, 1, 0, 0, tzinfo=timezone.utc),
            facility_id=123,
            patient_id=patient.ccmdd_patient_id,
            patient_phone="bad-phone",
            department_id=1,
            return_dates=[{"return_date": "2026-04-30"}],
            payload={},
        )
        turn_client = Mock()
        turn_client.import_contacts.return_value = []

        with (
            patch("synch.tasks.TurnAPIClient", return_value=turn_client),
            patch(
                "synch.tasks.django_timezone.localdate",
                return_value=datetime(2026, 4, 21).date(),
            ),
            self.assertLogs("synch.tasks", level="INFO") as logs,
        ):
            sync_appointment_dates_to_turn()

        turn_client.import_contacts.assert_not_called()
        self.assertEqual(
            logs.output,
            [
                "INFO:synch.tasks:Patient patient-bad-phone has an "
                "unparseable phone number, skipping Turn appointment sync.",
                "INFO:synch.tasks:Imported 0 appointment updates to Turn.",
            ],
        )

    def test_sync_appointment_dates_to_turn_uses_blank_facility_values_when_missing(
        self,
    ):
        patient = Patient.objects.create(
            ccmdd_patient_id="patient-missing-facility",
            date_created=datetime(2026, 4, 1, 0, 0, 1, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 1, 0, 0, 1, tzinfo=timezone.utc),
            payload={},
        )
        Prescription.objects.create(
            ccmdd_prescription_id="rx",
            date_created=datetime(2026, 4, 2, 1, 0, 0, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 2, 1, 0, 0, tzinfo=timezone.utc),
            facility_id=999,
            patient_id=patient.ccmdd_patient_id,
            patient_phone="0820000002",
            department_id=1,
            return_dates=[{"return_date": "2026-04-22"}],
            payload={},
        )
        turn_client = Mock()
        turn_client.import_contacts.return_value = []

        with (
            patch("synch.tasks.TurnAPIClient", return_value=turn_client),
            patch(
                "synch.tasks.django_timezone.localdate",
                return_value=datetime(2026, 4, 21).date(),
            ),
        ):
            sync_appointment_dates_to_turn()

        turn_client.import_contacts.assert_called_once_with(
            [
                {
                    "urn": "+27820000002",
                    "synch_next_appointment_date": "2026-04-22",
                    "synch_appointment_facility_name": "",
                    "synch_appointment_facility_latitude": "",
                    "synch_appointment_facility_longitude": "",
                }
            ]
        )
