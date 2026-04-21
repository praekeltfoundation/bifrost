from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import Mock, patch

from celery.schedules import crontab
from django.test import TestCase, override_settings
from django.utils.module_loading import import_string

from bifrost.celery import app
from lock.models import Lock
from synch.models import Patient, Prescription
from synch.tasks import healthcheck, sync_all, sync_patients, sync_prescriptions

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

    def test_configures_daily_sync_schedule(self):
        self.assertEqual(
            app.conf.beat_schedule["sync-ccmdd"],
            {
                "task": "synch.tasks.sync_all",
                "schedule": crontab(minute=0, hour=0),
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

    def test_sync_all_runs_patient_sync_before_prescription_sync(self):
        with (
            patch("synch.tasks.sync_patients") as sync_patients_mock,
            patch("synch.tasks.sync_prescriptions") as sync_prescriptions_mock,
        ):
            result = sync_all.delay()

        self.assertTrue(result.successful())
        sync_patients_mock.assert_called_once()
        sync_prescriptions_mock.assert_called_once()
        self.assertIs(
            sync_patients_mock.call_args.args[0],
            sync_prescriptions_mock.call_args.args[0],
        )

    def test_sync_all_does_not_run_prescriptions_when_patient_sync_fails(self):
        with (
            patch("synch.tasks.sync_patients", side_effect=RuntimeError("boom")),
            patch("synch.tasks.sync_prescriptions") as sync_prescriptions_mock,
            self.assertRaisesMessage(RuntimeError, "boom"),
        ):
            sync_all.delay()

        sync_prescriptions_mock.assert_not_called()

    def test_sync_all_skips_when_top_level_lock_is_already_held(self):
        Lock.acquire("sync-ccmdd")

        with (
            patch("synch.tasks.sync_patients") as sync_patients_mock,
            patch("synch.tasks.sync_prescriptions") as sync_prescriptions_mock,
            self.assertLogs("synch.tasks", level="WARNING") as logs,
        ):
            result = sync_all.delay()

        self.assertTrue(result.successful())
        sync_patients_mock.assert_not_called()
        sync_prescriptions_mock.assert_not_called()
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
