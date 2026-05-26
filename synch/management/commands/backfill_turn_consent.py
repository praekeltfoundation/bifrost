from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from time import sleep

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from synch.turn import TurnAPIClient, TurnAPIError

CONTACT_FIELDS = {
    "synch_reminders": "True",
    "contact_ndoh_privacy_policy": "true",
}
TEMPLATE_NAMESPACE = "415838db_3c2c_481c_9468_c1ee97c3b2c5"
TEMPLATE_NAME = "synch_service_confirmation_6"
TEMPLATE_LANGUAGE = "en"
DEFAULT_DELAY_SECONDS = 1.0
OUTCOME_FIELDNAMES = [
    "whatsapp_phone_number",
    "synch_appointment_facility_name",
    "status",
    "message_id",
    "error",
]
SUCCESS_STATUS = "success"
FAILURE_STATUS = "failure"


@dataclass(frozen=True)
class InputRow:
    whatsapp_phone_number: str
    facility_name: str


def _default_outcome_path(csv_path: Path) -> Path:
    return csv_path.with_name(f"{csv_path.stem}.turn_consent_backfill.csv")


def _normalize_contact_id(phone_number: str) -> str:
    return phone_number.lstrip("+")


class Command(BaseCommand):
    help = (
        "Backfill reminder consent fields in Turn from a CSV export and send the "
        "SynCH service confirmation template."
    )

    def add_arguments(self, parser) -> None:
        parser.add_argument("csv_path")
        parser.add_argument(
            "--execute",
            action="store_true",
            help="Perform live Turn updates and sends. Without this flag the command "
            "runs in preview mode.",
        )
        parser.add_argument(
            "--output",
            help="Optional path for the outcome ledger CSV.",
        )
        parser.add_argument(
            "--delay-seconds",
            type=float,
            default=DEFAULT_DELAY_SECONDS,
            help="Delay after each successful send during execute mode.",
        )

    def handle(self, *args, **options) -> None:
        csv_path = Path(options["csv_path"]).expanduser().resolve()
        if not csv_path.exists():
            raise CommandError(f"Input CSV not found: {csv_path}")

        outcome_path = (
            Path(options["output"]).expanduser().resolve()
            if options["output"]
            else _default_outcome_path(csv_path)
        )
        execute = bool(options["execute"])
        delay_seconds = float(options["delay_seconds"])

        input_rows = self._load_input_rows(csv_path)
        existing_outcomes = self._load_existing_outcomes(outcome_path)
        skipped_successes = sum(
            1
            for row in input_rows
            if existing_outcomes.get(row.whatsapp_phone_number, {}).get("status")
            == SUCCESS_STATUS
        )

        if not execute:
            actionable_rows = len(input_rows) - skipped_successes
            self.stdout.write(
                "Preview only. "
                f"Would process {actionable_rows} row(s), skip {skipped_successes} "
                f"successful row(s), and write outcomes to {outcome_path}."
            )
            return

        client = TurnAPIClient(
            base_url=settings.TURN_BASE_URL,
            token=settings.TURN_TOKEN,
        )
        outcomes = existing_outcomes.copy()
        processed_rows = 0

        for row in input_rows:
            if (
                outcomes.get(row.whatsapp_phone_number, {}).get("status")
                == SUCCESS_STATUS
            ):
                continue

            processed_rows += 1
            if not row.facility_name:
                error = "Missing facility name."
                self.stderr.write(f"{row.whatsapp_phone_number}: {error}")
                outcomes[row.whatsapp_phone_number] = {
                    "whatsapp_phone_number": row.whatsapp_phone_number,
                    "synch_appointment_facility_name": row.facility_name,
                    "status": FAILURE_STATUS,
                    "message_id": "",
                    "error": error,
                }
                continue

            try:
                client.update_contact_profile(
                    contact_id=_normalize_contact_id(row.whatsapp_phone_number),
                    fields=CONTACT_FIELDS,
                )
                message_id = client.send_template_message(
                    msisdn=row.whatsapp_phone_number,
                    template_namespace=TEMPLATE_NAMESPACE,
                    template_name=TEMPLATE_NAME,
                    template_language=TEMPLATE_LANGUAGE,
                    body_parameters=[row.facility_name],
                )
            except TurnAPIError as error:
                outcomes[row.whatsapp_phone_number] = {
                    "whatsapp_phone_number": row.whatsapp_phone_number,
                    "synch_appointment_facility_name": row.facility_name,
                    "status": FAILURE_STATUS,
                    "message_id": "",
                    "error": str(error),
                }
                self.stderr.write(f"{row.whatsapp_phone_number}: {error}")
                continue

            outcomes[row.whatsapp_phone_number] = {
                "whatsapp_phone_number": row.whatsapp_phone_number,
                "synch_appointment_facility_name": row.facility_name,
                "status": SUCCESS_STATUS,
                "message_id": message_id,
                "error": "",
            }
            sleep(delay_seconds)

        self._write_outcomes(outcome_path, input_rows, outcomes)
        self.stdout.write(
            f"Processed {processed_rows} row(s). "
            f"Outcome ledger written to {outcome_path}."
        )

    def _load_input_rows(self, csv_path: Path) -> list[InputRow]:
        with csv_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fieldnames = reader.fieldnames or []
            required_columns = {
                "whatsapp_phone_number",
                "synch_appointment_facility_name",
            }
            missing_columns = sorted(required_columns - set(fieldnames))
            if missing_columns:
                raise CommandError(
                    "Input CSV is missing required column(s): "
                    + ", ".join(missing_columns)
                )

            return [
                InputRow(
                    whatsapp_phone_number=(
                        row.get("whatsapp_phone_number") or ""
                    ).strip(),
                    facility_name=(
                        row.get("synch_appointment_facility_name") or ""
                    ).strip(),
                )
                for row in reader
            ]

    def _load_existing_outcomes(self, outcome_path: Path) -> dict[str, dict[str, str]]:
        if not outcome_path.exists():
            return {}

        with outcome_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            outcomes: dict[str, dict[str, str]] = {}
            for row in reader:
                phone_number = (row.get("whatsapp_phone_number") or "").strip()
                if not phone_number:
                    continue
                outcomes[phone_number] = {
                    fieldname: row.get(fieldname, "")
                    for fieldname in OUTCOME_FIELDNAMES
                }
            return outcomes

    def _write_outcomes(
        self,
        outcome_path: Path,
        input_rows: list[InputRow],
        outcomes: dict[str, dict[str, str]],
    ) -> None:
        with outcome_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=OUTCOME_FIELDNAMES)
            writer.writeheader()
            for row in input_rows:
                outcome = outcomes.get(row.whatsapp_phone_number)
                if outcome is None:
                    outcome = {
                        "whatsapp_phone_number": row.whatsapp_phone_number,
                        "synch_appointment_facility_name": row.facility_name,
                        "status": FAILURE_STATUS,
                        "message_id": "",
                        "error": "Row was not processed.",
                    }
                writer.writerow(outcome)
