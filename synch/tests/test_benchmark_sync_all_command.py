from __future__ import annotations

from datetime import datetime, timezone
from io import StringIO

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase

from synch.management.commands.benchmark_sync_all import Command
from synch.models import Facility, Patient, Prescription


class BenchmarkSyncAllCommandTests(TestCase):
    def test_defaults_match_recent_production_log_shape(self):
        parser = Command().create_parser("manage.py", "benchmark_sync_all")
        options = parser.parse_args([])

        self.assertEqual(options.facilities, 10732)
        self.assertEqual(options.patients, 75132)
        self.assertEqual(options.prescriptions, 82791)
        self.assertEqual(options.appointment_updates, 4)
        self.assertEqual(options.new_patients, 4)
        self.assertEqual(options.patient_updates, 5)
        self.assertEqual(options.prescription_updates, 5)
        self.assertEqual(options.phone_changes, 0)

    def test_benchmarks_sync_all_with_mocked_external_clients(self):
        stdout = StringIO()

        call_command(
            "benchmark_sync_all",
            "--facilities=5",
            "--patients=6",
            "--prescriptions=7",
            "--appointment-updates=5",
            "--new-patients=2",
            "--patient-updates=1",
            "--prescription-updates=2",
            "--phone-changes=1",
            stdout=stdout,
        )

        output = stdout.getvalue()
        self.assertIn("sync_all benchmark", output)
        self.assertIn("sync_appointment_dates_to_turn:", output)
        self.assertIn("queries", output)
        self.assertIn("select prescriptions for patient", output)
        self.assertIn("turn_import_rows: 5, 2, 1, 1", output)
        self.assertIn("rss_baseline_mib:", output)
        self.assertIn("rss_peak_mib:", output)
        self.assertIn("rss_after_sync_mib:", output)
        self.assertIn("rss_before_mib:", output)
        self.assertIn("rss_after_mib:", output)
        self.assertEqual(Facility.objects.count(), 5)
        self.assertEqual(Patient.objects.count(), 6)
        self.assertEqual(Prescription.objects.count(), 9)
        changed_patient = Patient.objects.get(
            ccmdd_patient_id="benchmark-sync-all-patient-00002"
        )
        self.assertEqual(changed_patient.active_messaging_phone_number, "+27820000002")
        self.assertIn("gender_description", changed_patient.payload)
        self.assertIn(
            "prescriber_name",
            Prescription.objects.get(
                ccmdd_prescription_id="benchmark-sync-all-prescription-00000"
            ).payload,
        )
        self.assertIn(
            "classification",
            Facility.objects.get(ccmdd_facility_id=90000000).payload,
        )

    def test_refuses_to_mix_with_non_benchmark_synch_data_by_default(self):
        Patient.objects.create(
            ccmdd_patient_id="real-patient",
            date_created=datetime(2026, 4, 1, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 1, tzinfo=timezone.utc),
            payload={},
        )

        with self.assertRaisesMessage(CommandError, "Non-benchmark synch data exists"):
            call_command(
                "benchmark_sync_all",
                "--facilities=1",
                "--patients=1",
                "--prescriptions=1",
                "--appointment-updates=1",
                "--new-patients=0",
                "--patient-updates=0",
                "--prescription-updates=0",
            )

    def test_seed_only_then_run_only_keeps_data_for_separate_process_measurement(self):
        options = (
            "--facilities=3",
            "--patients=4",
            "--prescriptions=5",
            "--appointment-updates=1",
            "--new-patients=1",
            "--patient-updates=1",
            "--prescription-updates=1",
            "--phone-changes=0",
        )
        seed_output = StringIO()
        call_command("benchmark_sync_all", *options, "--seed-only", stdout=seed_output)
        self.assertEqual(Prescription.objects.count(), 5)
        self.assertNotIn("Running sync_all benchmark", seed_output.getvalue())

        run_output = StringIO()
        call_command("benchmark_sync_all", *options, "--run-only", stdout=run_output)
        self.assertEqual(Prescription.objects.count(), 6)
        self.assertIn("rss_peak_mib:", run_output.getvalue())
