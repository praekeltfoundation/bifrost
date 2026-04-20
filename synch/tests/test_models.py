from __future__ import annotations

from datetime import datetime, timezone

from django.test import TestCase

from synch.models import Patient, Prescription


class PatientModelTests(TestCase):
    def test_string_representation_uses_patient_id(self):
        patient = Patient.objects.create(
            ccmdd_patient_id="90653BC3-DF69-E611-9D09-20689D5CEDFC",
            date_created=datetime(2016, 4, 8, 12, 48, 15, tzinfo=timezone.utc),
            date_updated=datetime(2016, 4, 29, 11, 25, 28, tzinfo=timezone.utc),
            payload={"surname": "wer"},
        )

        self.assertEqual(str(patient), patient.ccmdd_patient_id)


class PrescriptionModelTests(TestCase):
    def test_string_representation_uses_prescription_id(self):
        prescription = Prescription.objects.create(
            ccmdd_prescription_id="B2798F40-FA2C-F111-AD54-010101010000",
            date_created=datetime(2026, 3, 31, 14, 7, 57, 167000, tzinfo=timezone.utc),
            date_updated=datetime(2026, 3, 31, 14, 7, 57, 433000, tzinfo=timezone.utc),
            facility_id=937324,
            patient_id="D905C1E4-1962-E711-9D8C-7C5CF8BA146D",
            patient_phone="1231231233",
            department_id=123,
            return_dates=[
                {
                    "return_date": "2026-04-28",
                    "note": "Testing",
                    "description": "1 month",
                    "day_count": 28,
                }
            ],
            payload={"status_description": "Submitted"},
        )

        self.assertEqual(str(prescription), prescription.ccmdd_prescription_id)
