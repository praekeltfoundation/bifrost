from __future__ import annotations

import csv
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

from django.core.management import call_command
from django.test import SimpleTestCase, override_settings

TEST_TOKEN = "test-token"  # noqa: S105


@override_settings(
    TURN_BASE_URL="https://whatsapp.turn.io",
    TURN_TOKEN=TEST_TOKEN,
)
class BackfillTurnConsentCommandTests(SimpleTestCase):
    def write_csv(self, path: Path, rows: list[dict[str, str]]) -> None:
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=["whatsapp_phone_number", "synch_appointment_facility_name"],
            )
            writer.writeheader()
            writer.writerows(rows)

    def read_csv_rows(self, path: Path) -> list[dict[str, str]]:
        with path.open(newline="", encoding="utf-8") as handle:
            return list(csv.DictReader(handle))

    def test_preview_mode_is_default_and_does_not_write_outcome_file(self) -> None:
        with TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "patients.csv"
            self.write_csv(
                csv_path,
                [
                    {
                        "whatsapp_phone_number": "+27123456789",
                        "synch_appointment_facility_name": "Clinic A",
                    }
                ],
            )
            stdout = StringIO()

            with patch("synch.management.commands.backfill_turn_consent.TurnAPIClient"):
                call_command(
                    "backfill_turn_consent",
                    str(csv_path),
                    stdout=stdout,
                )

            self.assertIn("Preview only", stdout.getvalue())
            self.assertFalse(
                (Path(temp_dir) / "patients.turn_consent_backfill.csv").exists()
            )

    def test_execute_updates_contact_and_sends_template_then_writes_success_ledger(
        self,
    ) -> None:
        with TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "patients.csv"
            self.write_csv(
                csv_path,
                [
                    {
                        "whatsapp_phone_number": "+27123456789",
                        "synch_appointment_facility_name": "Clinic A",
                    }
                ],
            )
            stdout = StringIO()
            client = Mock()
            client.send_template_message.return_value = "wamid.123"

            with (
                patch(
                    "synch.management.commands.backfill_turn_consent.TurnAPIClient",
                    return_value=client,
                ),
                patch("synch.management.commands.backfill_turn_consent.sleep"),
            ):
                call_command(
                    "backfill_turn_consent",
                    str(csv_path),
                    "--execute",
                    stdout=stdout,
                )

            client.update_contact_profile.assert_called_once_with(
                contact_id="27123456789",
                fields={
                    "sync_reminders": "True",
                    "contact_ndoh_privacy_policy": "true",
                },
            )
            client.send_template_message.assert_called_once_with(
                msisdn="+27123456789",
                template_namespace="415838db_3c2c_481c_9468_c1ee97c3b2c5",
                template_name="synch_service_confirmation_6",
                template_language="en",
                body_parameters=["Clinic A"],
            )
            outcome_rows = self.read_csv_rows(
                Path(temp_dir) / "patients.turn_consent_backfill.csv"
            )
            self.assertEqual(
                outcome_rows,
                [
                    {
                        "whatsapp_phone_number": "+27123456789",
                        "synch_appointment_facility_name": "Clinic A",
                        "status": "success",
                        "message_id": "wamid.123",
                        "error": "",
                    }
                ],
            )
            self.assertIn("Processed 1 row(s)", stdout.getvalue())

    def test_execute_marks_missing_facility_name_as_failure(self) -> None:
        with TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "patients.csv"
            self.write_csv(
                csv_path,
                [
                    {
                        "whatsapp_phone_number": "+27123456789",
                        "synch_appointment_facility_name": "",
                    }
                ],
            )
            stdout = StringIO()
            stderr = StringIO()

            with patch("synch.management.commands.backfill_turn_consent.TurnAPIClient"):
                call_command(
                    "backfill_turn_consent",
                    str(csv_path),
                    "--execute",
                    stdout=stdout,
                    stderr=stderr,
                )

            outcome_rows = self.read_csv_rows(
                Path(temp_dir) / "patients.turn_consent_backfill.csv"
            )
            self.assertEqual(
                outcome_rows,
                [
                    {
                        "whatsapp_phone_number": "+27123456789",
                        "synch_appointment_facility_name": "",
                        "status": "failure",
                        "message_id": "",
                        "error": "Missing facility name.",
                    }
                ],
            )
            self.assertIn("Missing facility name", stderr.getvalue())

    def test_execute_skips_rows_already_marked_success_in_outcome_file(self) -> None:
        with TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "patients.csv"
            self.write_csv(
                csv_path,
                [
                    {
                        "whatsapp_phone_number": "+27123456789",
                        "synch_appointment_facility_name": "Clinic A",
                    },
                    {
                        "whatsapp_phone_number": "+27987654321",
                        "synch_appointment_facility_name": "Clinic B",
                    },
                ],
            )
            outcome_path = Path(temp_dir) / "patients.turn_consent_backfill.csv"
            with outcome_path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "whatsapp_phone_number",
                        "synch_appointment_facility_name",
                        "status",
                        "message_id",
                        "error",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "whatsapp_phone_number": "+27123456789",
                        "synch_appointment_facility_name": "Clinic A",
                        "status": "success",
                        "message_id": "wamid.old",
                        "error": "",
                    }
                )
            client = Mock()
            client.send_template_message.return_value = "wamid.new"

            with (
                patch(
                    "synch.management.commands.backfill_turn_consent.TurnAPIClient",
                    return_value=client,
                ),
                patch("synch.management.commands.backfill_turn_consent.sleep"),
            ):
                call_command(
                    "backfill_turn_consent",
                    str(csv_path),
                    "--execute",
                )

            client.update_contact_profile.assert_called_once_with(
                contact_id="27987654321",
                fields={
                    "sync_reminders": "True",
                    "contact_ndoh_privacy_policy": "true",
                },
            )
            outcome_rows = self.read_csv_rows(outcome_path)
            self.assertEqual(
                outcome_rows,
                [
                    {
                        "whatsapp_phone_number": "+27123456789",
                        "synch_appointment_facility_name": "Clinic A",
                        "status": "success",
                        "message_id": "wamid.old",
                        "error": "",
                    },
                    {
                        "whatsapp_phone_number": "+27987654321",
                        "synch_appointment_facility_name": "Clinic B",
                        "status": "success",
                        "message_id": "wamid.new",
                        "error": "",
                    },
                ],
            )
