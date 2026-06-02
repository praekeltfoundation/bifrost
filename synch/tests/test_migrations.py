from __future__ import annotations

from datetime import datetime, timezone
from typing import ClassVar

from django.db import connection
from django.db.migrations.executor import MigrationExecutor
from django.test import TransactionTestCase


class PatientActiveMessagingPhoneNumberMigrationTests(TransactionTestCase):
    migrate_from: ClassVar[list[tuple[str, str]]] = [
        ("synch", "0007_create_otp_delivery_throttle_cache")
    ]
    migrate_to: ClassVar[list[tuple[str, str]]] = [
        ("synch", "0008_patient_active_messaging_phone_number")
    ]

    def test_backfills_invited_patients_with_current_messaging_phone_number(self):
        executor = MigrationExecutor(connection)
        leaf_targets = executor.loader.graph.leaf_nodes()
        migrate_from = self._replace_synch_target(leaf_targets, self.migrate_from[0])
        migrate_to = self._replace_synch_target(leaf_targets, self.migrate_to[0])

        try:
            executor.migrate(migrate_from)
            old_apps = executor.loader.project_state(migrate_from).apps

            Patient = old_apps.get_model("synch", "Patient")
            Prescription = old_apps.get_model("synch", "Prescription")
            invited_patient = Patient.objects.create(
                ccmdd_patient_id="invited-patient",
                date_created=datetime(2026, 4, 1, 0, 0, 1, tzinfo=timezone.utc),
                date_updated=datetime(2026, 4, 1, 0, 5, 0, tzinfo=timezone.utc),
                invite_sent=True,
                payload={},
            )
            uninvited_patient = Patient.objects.create(
                ccmdd_patient_id="uninvited-patient",
                date_created=datetime(2026, 4, 1, 0, 0, 1, tzinfo=timezone.utc),
                date_updated=datetime(2026, 4, 1, 0, 5, 0, tzinfo=timezone.utc),
                invite_sent=False,
                payload={},
            )
            Prescription.objects.create(
                ccmdd_prescription_id="invited-old-rx",
                date_created=datetime(2026, 4, 1, 1, 0, 0, tzinfo=timezone.utc),
                date_updated=datetime(2026, 4, 1, 1, 0, 0, tzinfo=timezone.utc),
                facility_id=1,
                patient_id=invited_patient.ccmdd_patient_id,
                patient_phone="0820000001",
                department_id=1,
                return_dates=[],
                payload={},
            )
            Prescription.objects.create(
                ccmdd_prescription_id="invited-new-invalid-rx",
                date_created=datetime(2026, 4, 2, 1, 0, 0, tzinfo=timezone.utc),
                date_updated=datetime(2026, 4, 2, 1, 0, 0, tzinfo=timezone.utc),
                facility_id=1,
                patient_id=invited_patient.ccmdd_patient_id,
                patient_phone="not-a-phone-number",
                department_id=1,
                return_dates=[],
                payload={},
            )
            Prescription.objects.create(
                ccmdd_prescription_id="uninvited-rx",
                date_created=datetime(2026, 4, 2, 1, 0, 0, tzinfo=timezone.utc),
                date_updated=datetime(2026, 4, 2, 1, 0, 0, tzinfo=timezone.utc),
                facility_id=1,
                patient_id=uninvited_patient.ccmdd_patient_id,
                patient_phone="0820000002",
                department_id=1,
                return_dates=[],
                payload={},
            )

            executor = MigrationExecutor(connection)
            executor.migrate(migrate_to)
            new_apps = executor.loader.project_state(migrate_to).apps
            MigratedPatient = new_apps.get_model("synch", "Patient")

            invited_patient = MigratedPatient.objects.get(
                ccmdd_patient_id="invited-patient"
            )
            uninvited_patient = MigratedPatient.objects.get(
                ccmdd_patient_id="uninvited-patient"
            )

            self.assertEqual(
                invited_patient.active_messaging_phone_number,
                "+27820000001",
            )
            self.assertEqual(uninvited_patient.active_messaging_phone_number, "")
        finally:
            executor = MigrationExecutor(connection)
            executor.migrate(leaf_targets)

    def _replace_synch_target(
        self,
        targets: list[tuple[str, str]],
        synch_target: tuple[str, str],
    ) -> list[tuple[str, str]]:
        return [target if target[0] != "synch" else synch_target for target in targets]
