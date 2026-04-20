from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import Mock, patch

from celery.schedules import crontab
from django.test import TestCase, override_settings
from django.utils.module_loading import import_string

from bifrost.celery import app
from lock.models import Lock
from synch.models import Patient
from synch.tasks import healthcheck, sync_patients

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

    def test_configures_daily_patient_sync_schedule(self):
        self.assertEqual(
            app.conf.beat_schedule["sync-patients"],
            {
                "task": "synch.tasks.sync_patients",
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

    def test_sync_patients_skips_when_lock_is_already_held(self):
        Lock.acquire("sync-patients")
        client = Mock()

        with (
            patch("synch.tasks.CCMDDAPIClient", return_value=client),
            self.assertLogs("synch.tasks", level="WARNING") as logs,
        ):
            sync_patients.delay()

        client.iter_limited_patients.assert_not_called()
        self.assertEqual(
            logs.output,
            [
                "WARNING:synch.tasks:"
                "Skipping patient sync because lock 'sync-patients' is already held."
            ],
        )
