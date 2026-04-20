from __future__ import annotations

from django.contrib import admin
from django.test import TestCase

from synch.models import Patient, Prescription


class PatientAdminTests(TestCase):
    def test_patient_model_is_registered_in_admin(self):
        self.assertIn(Patient, admin.site._registry)

    def test_patient_admin_lists_basic_sync_fields(self):
        model_admin = admin.site._registry[Patient]

        self.assertEqual(
            model_admin.list_display,
            ("ccmdd_patient_id", "date_created", "date_updated"),
        )
        self.assertEqual(model_admin.search_fields, ("ccmdd_patient_id",))


class PrescriptionAdminTests(TestCase):
    def test_prescription_model_is_registered_in_admin(self):
        self.assertIn(Prescription, admin.site._registry)

    def test_prescription_admin_lists_basic_sync_fields(self):
        model_admin = admin.site._registry[Prescription]

        self.assertEqual(
            model_admin.list_display,
            ("ccmdd_prescription_id", "patient_id", "date_created", "date_updated"),
        )
        self.assertEqual(
            model_admin.search_fields, ("ccmdd_prescription_id", "patient_id")
        )
