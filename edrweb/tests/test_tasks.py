from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import Mock, patch

from celery.exceptions import Retry
from celery.schedules import crontab
from django.test import TestCase, override_settings

from bifrost.celery import app
from edrweb.models import EDRWebPatient
from edrweb.tasks import (
    EDRWEB_APPOINTMENT_REMINDER_DELTA_LOCK_KEY,
    sync_appointment_reminder_delta,
    sync_appointment_reminder_full_reconciliation,
)
from lock.models import Lock

TEST_PASSWORD = "test-password"  # noqa: S105


class EDRWebCeleryConfigurationTests(TestCase):
    def test_configures_four_hour_delta_sync_schedule(self):
        self.assertEqual(
            app.conf.beat_schedule["sync-edrweb-appointment-reminder-delta"],
            {
                "task": "edrweb.tasks.sync_appointment_reminder_delta",
                "schedule": crontab(minute=0, hour="*/4"),
            },
        )

    def test_configures_weekly_full_reconciliation_schedule(self):
        self.assertEqual(
            app.conf.beat_schedule[
                "sync-edrweb-appointment-reminder-full-reconciliation"
            ],
            {
                "task": "edrweb.tasks.sync_appointment_reminder_full_reconciliation",
                "schedule": crontab(minute=0, hour=2, day_of_week="monday"),
            },
        )


@override_settings(
    CELERY_TASK_ALWAYS_EAGER=True,
    CELERY_TASK_EAGER_PROPAGATES=True,
    EDRWEB_BASE_URL="https://staging.edrweb.net/api",
    EDRWEB_USERNAME="api-user",
    EDRWEB_PASSWORD=TEST_PASSWORD,
)
class SyncAppointmentReminderDeltaTaskTests(TestCase):
    @override_settings(EDRWEB_BASE_URL="")
    def test_skips_when_api_config_is_missing(self):
        with (
            patch("edrweb.tasks.EDRWebAPIClient") as client_class,
            self.assertLogs("edrweb.tasks", level="WARNING") as logs,
        ):
            result = sync_appointment_reminder_delta.delay()

        self.assertTrue(result.successful())
        client_class.assert_not_called()
        self.assertFalse(Lock.objects.exists())
        self.assertEqual(
            logs.output,
            [
                "WARNING:edrweb.tasks:Skipping EDRWeb appointment reminder delta "
                "because EDRWEB_BASE_URL, EDRWEB_USERNAME, or EDRWEB_PASSWORD "
                "is not configured."
            ],
        )

    def test_creates_records_and_strips_modeled_fields_from_payload(self):
        client = Mock()
        client.iter_appointment_reminder_records.return_value = iter(
            [
                {
                    "PersonId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
                    "PhoneNumber": "+27721234567",
                    "UpdatedAt": "2026-05-30T14:22:00.000+02:00",
                    "Appointments": [
                        {
                            "AppointmentDate": "2026-06-20",
                            "Facility": {"FacilityName": "WC BLUE DOWNS CLINIC"},
                        }
                    ],
                    "FirstName": "Test",
                }
            ]
        )

        with (
            patch("edrweb.tasks.EDRWebAPIClient", return_value=client),
            self.assertLogs("edrweb.tasks", level="INFO") as logs,
        ):
            sync_appointment_reminder_delta.delay()

        patient = EDRWebPatient.objects.get()
        self.assertEqual(
            patient.patient_id,
            "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        )
        self.assertEqual(patient.phone_number, "+27721234567")
        self.assertEqual(
            patient.updated_at,
            datetime(2026, 5, 30, 12, 22, tzinfo=timezone.utc),
        )
        self.assertEqual(
            patient.appointments,
            [
                {
                    "AppointmentDate": "2026-06-20",
                    "Facility": {"FacilityName": "WC BLUE DOWNS CLINIC"},
                }
            ],
        )
        self.assertEqual(patient.payload, {"FirstName": "Test"})
        client.iter_appointment_reminder_records.assert_called_once_with(
            updated_since=None
        )
        self.assertEqual(
            logs.output,
            ["INFO:edrweb.tasks:Synced EDRWeb appointment reminder records: 1."],
        )

    def test_fetches_from_just_before_latest_stored_update(self):
        EDRWebPatient.objects.create(
            patient_id="existing-patient",
            phone_number="+27721234567",
            updated_at=datetime(2026, 5, 30, 12, 22, tzinfo=timezone.utc),
            appointments=[],
            payload={},
        )
        client = Mock()
        client.iter_appointment_reminder_records.return_value = iter([])

        with patch("edrweb.tasks.EDRWebAPIClient", return_value=client):
            sync_appointment_reminder_delta.delay()

        client.iter_appointment_reminder_records.assert_called_once_with(
            updated_since=datetime(2026, 5, 30, 12, 21, 59, tzinfo=timezone.utc),
        )

    def test_treats_missing_phone_number_and_appointments_as_blank_snapshot_fields(
        self,
    ):
        client = Mock()
        client.iter_appointment_reminder_records.return_value = iter(
            [
                {
                    "PersonId": "patient-with-missing-optional-fields",
                    "UpdatedAt": "2026-05-30T14:22:00.000+02:00",
                }
            ]
        )

        with patch("edrweb.tasks.EDRWebAPIClient", return_value=client):
            sync_appointment_reminder_delta.delay()

        patient = EDRWebPatient.objects.get()
        self.assertEqual(patient.phone_number, "")
        self.assertEqual(patient.appointments, [])
        self.assertEqual(patient.payload, {})

    def test_rolls_back_when_appointments_is_not_a_list(self):
        client = Mock()
        client.iter_appointment_reminder_records.return_value = iter(
            [
                {
                    "PersonId": "valid-patient",
                    "UpdatedAt": "2026-05-30T14:22:00.000+02:00",
                },
                {
                    "PersonId": "invalid-patient",
                    "UpdatedAt": "2026-05-30T14:23:00.000+02:00",
                    "Appointments": "not-a-list",
                },
            ]
        )

        with (
            patch("edrweb.tasks.EDRWebAPIClient", return_value=client),
            self.assertRaisesMessage(
                ValueError,
                "EDRWeb Appointments field must be a list.",
            ),
        ):
            sync_appointment_reminder_delta.delay()

        self.assertFalse(EDRWebPatient.objects.exists())

    def test_rolls_back_when_person_id_is_missing(self):
        client = Mock()
        client.iter_appointment_reminder_records.return_value = iter(
            [
                {
                    "UpdatedAt": "2026-05-30T14:22:00.000+02:00",
                }
            ]
        )

        with (
            patch("edrweb.tasks.EDRWebAPIClient", return_value=client),
            self.assertRaisesMessage(
                ValueError,
                "EDRWeb PersonId field is required.",
            ),
        ):
            sync_appointment_reminder_delta.delay()

        self.assertFalse(EDRWebPatient.objects.exists())

    def test_rolls_back_when_updated_at_is_missing(self):
        client = Mock()
        client.iter_appointment_reminder_records.return_value = iter(
            [
                {
                    "PersonId": "patient-without-updated-at",
                }
            ]
        )

        with (
            patch("edrweb.tasks.EDRWebAPIClient", return_value=client),
            self.assertRaisesMessage(
                ValueError,
                "EDRWeb UpdatedAt field is required.",
            ),
        ):
            sync_appointment_reminder_delta.delay()

        self.assertFalse(EDRWebPatient.objects.exists())

    def test_rolls_back_when_updated_at_has_no_timezone_offset(self):
        client = Mock()
        client.iter_appointment_reminder_records.return_value = iter(
            [
                {
                    "PersonId": "patient-with-naive-updated-at",
                    "UpdatedAt": "2026-05-30T14:22:00",
                }
            ]
        )

        with (
            patch("edrweb.tasks.EDRWebAPIClient", return_value=client),
            self.assertRaisesMessage(
                ValueError,
                "EDRWeb UpdatedAt field must include a timezone offset.",
            ),
        ):
            sync_appointment_reminder_delta.delay()

        self.assertFalse(EDRWebPatient.objects.exists())

    def test_older_duplicate_record_does_not_regress_snapshot(self):
        EDRWebPatient.objects.create(
            patient_id="duplicate-patient",
            phone_number="+27720000001",
            updated_at=datetime(2026, 5, 30, 12, 22, tzinfo=timezone.utc),
            appointments=[{"AppointmentDate": "2026-06-20"}],
            payload={"FirstName": "Current"},
        )
        client = Mock()
        client.iter_appointment_reminder_records.return_value = iter(
            [
                {
                    "PersonId": "duplicate-patient",
                    "PhoneNumber": "+27720000002",
                    "UpdatedAt": "2026-05-30T14:21:00.000+02:00",
                    "Appointments": [{"AppointmentDate": "2026-06-19"}],
                    "FirstName": "Old",
                }
            ]
        )

        with patch("edrweb.tasks.EDRWebAPIClient", return_value=client):
            sync_appointment_reminder_delta.delay()

        patient = EDRWebPatient.objects.get(patient_id="duplicate-patient")
        self.assertEqual(patient.phone_number, "+27720000001")
        self.assertEqual(
            patient.updated_at,
            datetime(2026, 5, 30, 12, 22, tzinfo=timezone.utc),
        )
        self.assertEqual(patient.appointments, [{"AppointmentDate": "2026-06-20"}])
        self.assertEqual(patient.payload, {"FirstName": "Current"})

    def test_reactivates_inactive_patient_when_record_returns(self):
        EDRWebPatient.objects.create(
            patient_id="returning-patient",
            phone_number="+27720000001",
            updated_at=datetime(2026, 5, 30, 12, 22, tzinfo=timezone.utc),
            is_active=False,
            feed_removed_at=datetime(2026, 6, 9, 2, 0, tzinfo=timezone.utc),
            appointments=[],
            payload={},
        )
        client = Mock()
        client.iter_appointment_reminder_records.return_value = iter(
            [
                {
                    "PersonId": "returning-patient",
                    "PhoneNumber": "+27720000002",
                    "UpdatedAt": "2026-06-02T14:22:00.000+02:00",
                    "Appointments": [{"AppointmentDate": "2026-06-20"}],
                }
            ]
        )

        with patch("edrweb.tasks.EDRWebAPIClient", return_value=client):
            sync_appointment_reminder_delta.delay()

        patient = EDRWebPatient.objects.get(patient_id="returning-patient")
        self.assertTrue(patient.is_active)
        self.assertIsNone(patient.feed_removed_at)
        self.assertEqual(patient.phone_number, "+27720000002")
        self.assertEqual(patient.appointments, [{"AppointmentDate": "2026-06-20"}])

    def test_reactivates_inactive_patient_when_older_record_returns(self):
        EDRWebPatient.objects.create(
            patient_id="returning-patient",
            phone_number="+27720000001",
            updated_at=datetime(2026, 6, 2, 12, 22, tzinfo=timezone.utc),
            is_active=False,
            feed_removed_at=datetime(2026, 6, 9, 2, 0, tzinfo=timezone.utc),
            appointments=[{"AppointmentDate": "2026-06-20"}],
            payload={"FirstName": "Current"},
        )
        client = Mock()
        client.iter_appointment_reminder_records.return_value = iter(
            [
                {
                    "PersonId": "returning-patient",
                    "PhoneNumber": "+27720000002",
                    "UpdatedAt": "2026-05-30T14:22:00.000+02:00",
                    "Appointments": [{"AppointmentDate": "2026-06-19"}],
                    "FirstName": "Old",
                }
            ]
        )

        with patch("edrweb.tasks.EDRWebAPIClient", return_value=client):
            sync_appointment_reminder_delta.delay()

        patient = EDRWebPatient.objects.get(patient_id="returning-patient")
        self.assertTrue(patient.is_active)
        self.assertIsNone(patient.feed_removed_at)
        self.assertEqual(patient.phone_number, "+27720000001")
        self.assertEqual(
            patient.updated_at, datetime(2026, 6, 2, 12, 22, tzinfo=timezone.utc)
        )
        self.assertEqual(patient.appointments, [{"AppointmentDate": "2026-06-20"}])
        self.assertEqual(patient.payload, {"FirstName": "Current"})

    def test_skips_when_delta_lock_is_already_held(self):
        Lock.acquire(EDRWEB_APPOINTMENT_REMINDER_DELTA_LOCK_KEY)

        with (
            patch("edrweb.tasks.EDRWebAPIClient") as client_class,
            self.assertLogs("edrweb.tasks", level="WARNING") as logs,
        ):
            result = sync_appointment_reminder_delta.delay()

        self.assertTrue(result.successful())
        client_class.assert_not_called()
        self.assertEqual(
            logs.output,
            [
                "WARNING:edrweb.tasks:Skipping EDRWeb appointment reminder delta "
                "because lock 'sync-edrweb-appointment-reminders' is already "
                "held."
            ],
        )


@override_settings(
    CELERY_TASK_ALWAYS_EAGER=True,
    CELERY_TASK_EAGER_PROPAGATES=True,
    EDRWEB_BASE_URL="https://staging.edrweb.net/api",
    EDRWEB_USERNAME="api-user",
    EDRWEB_PASSWORD=TEST_PASSWORD,
)
class SyncAppointmentReminderFullReconciliationTaskTests(TestCase):
    def test_marks_patients_missing_from_completed_full_feed_as_inactive(self):
        EDRWebPatient.objects.create(
            patient_id="removed-patient",
            phone_number="+27720000001",
            updated_at=datetime(2026, 5, 30, 12, 22, tzinfo=timezone.utc),
            appointments=[],
            payload={},
        )
        EDRWebPatient.objects.create(
            patient_id="current-patient",
            phone_number="+27720000002",
            updated_at=datetime(2026, 5, 30, 12, 23, tzinfo=timezone.utc),
            appointments=[],
            payload={},
        )
        client = Mock()
        client.iter_appointment_reminder_records.return_value = iter(
            [
                {
                    "PersonId": "current-patient",
                    "PhoneNumber": "+27720000002",
                    "UpdatedAt": "2026-06-02T14:22:00.000+02:00",
                    "Appointments": [],
                }
            ]
        )
        removed_at = datetime(2026, 6, 9, 2, 0, tzinfo=timezone.utc)

        with (
            patch("edrweb.tasks.EDRWebAPIClient", return_value=client),
            patch("edrweb.tasks.django_timezone.now", return_value=removed_at),
        ):
            sync_appointment_reminder_full_reconciliation.delay()

        removed_patient = EDRWebPatient.objects.get(patient_id="removed-patient")
        self.assertFalse(removed_patient.is_active)
        self.assertEqual(removed_patient.feed_removed_at, removed_at)
        current_patient = EDRWebPatient.objects.get(patient_id="current-patient")
        self.assertTrue(current_patient.is_active)
        self.assertIsNone(current_patient.feed_removed_at)
        client.iter_appointment_reminder_records.assert_called_once_with(
            updated_since=None
        )

    def test_retries_when_shared_lock_is_already_held(self):
        Lock.acquire(EDRWEB_APPOINTMENT_REMINDER_DELTA_LOCK_KEY)

        with (
            patch("edrweb.tasks.EDRWebAPIClient") as client_class,
            self.assertRaises(Retry),
        ):
            sync_appointment_reminder_full_reconciliation.delay()

        client_class.assert_not_called()

    def test_does_not_mark_removals_when_full_feed_fails_before_completion(self):
        EDRWebPatient.objects.create(
            patient_id="current-patient",
            phone_number="+27720000002",
            updated_at=datetime(2026, 5, 30, 12, 23, tzinfo=timezone.utc),
            appointments=[],
            payload={},
        )
        EDRWebPatient.objects.create(
            patient_id="possibly-removed-patient",
            phone_number="+27720000003",
            updated_at=datetime(2026, 5, 30, 12, 24, tzinfo=timezone.utc),
            appointments=[],
            payload={},
        )

        def incomplete_feed():
            yield {
                "PersonId": "current-patient",
                "PhoneNumber": "+27720000004",
                "UpdatedAt": "2026-06-02T14:22:00.000+02:00",
                "Appointments": [],
            }
            raise RuntimeError("feed failed")

        client = Mock()
        client.iter_appointment_reminder_records.return_value = incomplete_feed()

        with (
            patch("edrweb.tasks.EDRWebAPIClient", return_value=client),
            self.assertRaisesMessage(RuntimeError, "feed failed"),
        ):
            sync_appointment_reminder_full_reconciliation.delay()

        current_patient = EDRWebPatient.objects.get(patient_id="current-patient")
        self.assertEqual(current_patient.phone_number, "+27720000002")
        self.assertTrue(current_patient.is_active)
        self.assertIsNone(current_patient.feed_removed_at)
        possibly_removed_patient = EDRWebPatient.objects.get(
            patient_id="possibly-removed-patient"
        )
        self.assertTrue(possibly_removed_patient.is_active)
        self.assertIsNone(possibly_removed_patient.feed_removed_at)
