from __future__ import annotations

from datetime import datetime, timezone

from django.test import TestCase

from synch.models import Patient


class PatientModelTests(TestCase):
    def test_string_representation_uses_patient_id(self):
        patient = Patient.objects.create(
            ccmdd_patient_id="90653BC3-DF69-E611-9D09-20689D5CEDFC",
            date_created=datetime(2016, 4, 8, 12, 48, 15, tzinfo=timezone.utc),
            date_updated=datetime(2016, 4, 29, 11, 25, 28, tzinfo=timezone.utc),
            payload={"surname": "wer"},
        )

        self.assertEqual(str(patient), patient.ccmdd_patient_id)
