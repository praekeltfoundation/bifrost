from __future__ import annotations

from datetime import datetime, timezone

from django.test import TestCase

from edrweb.models import EDRWebPatient


class EDRWebPatientModelTests(TestCase):
    def test_turn_sync_row_for_active_patient_includes_context_fields(self):
        patient = self._create_patient(
            phone_number="0721234567",
            appointments=[
                {
                    "AppointmentDate": "2026-07-20",
                    "Facility": {"FacilityName": "Later Clinic"},
                },
                {
                    "AppointmentDate": "2026-06-20",
                    "Facility": {
                        "FacilityName": "WC BLUE DOWNS CLINIC",
                        "Latitude": -33.9744,
                        "Longitude": 18.7032,
                    },
                },
            ],
        )

        self.assertEqual(
            patient.get_turn_sync_row(),
            {
                "urn": "+27721234567",
                "edrweb_patient_id": "edrweb-patient-1",
                "edrweb_next_appointment_date": "2026-06-20",
                "edrweb_appointment_facility_name": "WC BLUE DOWNS CLINIC",
                "edrweb_appointment_facility_latitude": -33.9744,
                "edrweb_appointment_facility_longitude": 18.7032,
            },
        )

    def test_turn_sync_row_for_active_patient_without_appointment_sends_blank_context(
        self,
    ):
        patient = self._create_patient(appointments=[])

        self.assertEqual(
            patient.get_turn_sync_row(),
            {
                "urn": "+27721234567",
                "edrweb_patient_id": "edrweb-patient-1",
                "edrweb_next_appointment_date": "",
                "edrweb_appointment_facility_name": "",
                "edrweb_appointment_facility_latitude": "",
                "edrweb_appointment_facility_longitude": "",
            },
        )

    def test_turn_sync_row_keeps_facility_fields_blank_when_appointment_lacks_facility(
        self,
    ):
        patient = self._create_patient(
            appointments=[
                {
                    "AppointmentDate": "2026-06-20",
                }
            ],
        )

        self.assertEqual(
            patient.get_turn_sync_row(),
            {
                "urn": "+27721234567",
                "edrweb_patient_id": "edrweb-patient-1",
                "edrweb_next_appointment_date": "2026-06-20",
                "edrweb_appointment_facility_name": "",
                "edrweb_appointment_facility_latitude": "",
                "edrweb_appointment_facility_longitude": "",
            },
        )

    def test_turn_sync_row_for_inactive_patient_only_disables_reminders(self):
        patient = self._create_patient(
            is_active=False,
            appointments=[
                {
                    "AppointmentDate": "2026-06-20",
                    "Facility": {"FacilityName": "WC BLUE DOWNS CLINIC"},
                }
            ],
        )

        self.assertEqual(
            patient.get_turn_sync_row(),
            {
                "urn": "+27721234567",
                "edrweb_reminders": "False",
            },
        )

    def test_turn_sync_row_for_patient_without_usable_phone_returns_none(self):
        patient = self._create_patient(phone_number="not-a-phone-number")

        self.assertIsNone(patient.get_turn_sync_row())

    def _create_patient(
        self,
        *,
        phone_number: str = "+27721234567",
        is_active: bool = True,
        appointments: list[object] | None = None,
    ) -> EDRWebPatient:
        return EDRWebPatient.objects.create(
            patient_id="edrweb-patient-1",
            phone_number=phone_number,
            updated_at=datetime(2026, 5, 30, 12, 22, tzinfo=timezone.utc),
            is_active=is_active,
            appointments=appointments if appointments is not None else [],
            payload={},
        )
