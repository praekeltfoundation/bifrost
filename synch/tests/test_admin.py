from __future__ import annotations

from datetime import datetime, timezone

from django.contrib import admin
from django.test import RequestFactory, TestCase

from synch.models import Facility, Patient, Prescription


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

    def test_patient_admin_keeps_internal_messaging_state_read_only(self):
        model_admin = admin.site._registry[Patient]
        patient = Patient.objects.create(
            ccmdd_patient_id="patient-1",
            date_created=datetime(2026, 5, 1, tzinfo=timezone.utc),
            date_updated=datetime(2026, 5, 2, tzinfo=timezone.utc),
            invite_sent=True,
            active_messaging_phone_number="+27820000001",
            payload={},
        )
        request = RequestFactory().get("/admin/synch/patient/1/change/")

        form = model_admin.get_form(request, obj=patient)(instance=patient)

        self.assertIn("ccmdd_patient_id", form.fields)
        self.assertIn("date_created", form.fields)
        self.assertIn("date_updated", form.fields)
        self.assertNotIn("invite_sent", form.fields)
        self.assertNotIn("active_messaging_phone_number", form.fields)
        self.assertNotIn("payload", form.fields)
        self.assertIn("invite_sent", model_admin.readonly_fields)
        self.assertIn("active_messaging_phone_number", model_admin.readonly_fields)
        self.assertIn("turn_appointment_context_urn", model_admin.readonly_fields)
        self.assertIn(
            "turn_appointment_context_patient_id", model_admin.readonly_fields
        )
        self.assertIn(
            "turn_appointment_context_next_appointment_date",
            model_admin.readonly_fields,
        )
        self.assertIn(
            "turn_appointment_context_facility_name", model_admin.readonly_fields
        )
        self.assertIn(
            "turn_appointment_context_facility_latitude", model_admin.readonly_fields
        )
        self.assertIn(
            "turn_appointment_context_facility_longitude", model_admin.readonly_fields
        )
        self.assertIn("turn_appointment_context_synced_at", model_admin.readonly_fields)
        self.assertIn("payload", model_admin.readonly_fields)


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

    def test_prescription_admin_allows_editing_return_dates(self):
        model_admin = admin.site._registry[Prescription]
        prescription = Prescription.objects.create(
            ccmdd_prescription_id="rx-1",
            date_created=datetime(2026, 5, 1, tzinfo=timezone.utc),
            date_updated=datetime(2026, 5, 1, tzinfo=timezone.utc),
            facility_id=1,
            patient_id="patient-1",
            patient_phone="0820000001",
            department_id=1,
            return_dates=[],
            payload={},
        )
        request = RequestFactory().get("/admin/synch/prescription/1/change/")

        form = model_admin.get_form(request, obj=prescription)(instance=prescription)

        self.assertIn("return_dates", form.fields)
        self.assertNotIn("return_dates", model_admin.readonly_fields)

    def test_prescription_admin_accepts_upstream_shaped_return_dates(self):
        model_admin = admin.site._registry[Prescription]
        prescription = Prescription.objects.create(
            ccmdd_prescription_id="rx-1",
            date_created=datetime(2026, 5, 1, tzinfo=timezone.utc),
            date_updated=datetime(2026, 5, 1, tzinfo=timezone.utc),
            facility_id=1,
            patient_id="patient-1",
            patient_phone="0820000001",
            department_id=1,
            return_dates=[],
            payload={},
        )
        request = RequestFactory().post("/admin/synch/prescription/1/change/")
        form_class = model_admin.get_form(request, obj=prescription)
        form = form_class(
            data={
                "ccmdd_prescription_id": prescription.ccmdd_prescription_id,
                "date_created_0": "2026-05-01",
                "date_created_1": "00:00:00",
                "date_updated_0": "2026-05-01",
                "date_updated_1": "00:00:00",
                "facility_id": "1",
                "patient_id": "patient-1",
                "patient_phone": "0820000001",
                "department_id": "1",
                "return_dates": '[{"note": "Testing", "day_count": 28, "description": "1 Month", "return_date": "2026-06-04"}]',  # noqa: E501
                "payload": "{}",
            },
            instance=prescription,
        )

        self.assertTrue(form.is_valid(), form.errors)

    def test_prescription_admin_rejects_return_dates_with_unknown_keys(self):
        model_admin = admin.site._registry[Prescription]
        prescription = Prescription.objects.create(
            ccmdd_prescription_id="rx-1",
            date_created=datetime(2026, 5, 1, tzinfo=timezone.utc),
            date_updated=datetime(2026, 5, 1, tzinfo=timezone.utc),
            facility_id=1,
            patient_id="patient-1",
            patient_phone="0820000001",
            department_id=1,
            return_dates=[],
            payload={},
        )
        request = RequestFactory().post("/admin/synch/prescription/1/change/")
        form_class = model_admin.get_form(request, obj=prescription)
        form = form_class(
            data={
                "ccmdd_prescription_id": prescription.ccmdd_prescription_id,
                "date_created_0": "2026-05-01",
                "date_created_1": "00:00:00",
                "date_updated_0": "2026-05-01",
                "date_updated_1": "00:00:00",
                "facility_id": "1",
                "patient_id": "patient-1",
                "patient_phone": "0820000001",
                "department_id": "1",
                "return_dates": '[{"return_date": "2026-06-04", "foo": "bar"}]',
                "payload": "{}",
            },
            instance=prescription,
        )

        self.assertFalse(form.is_valid())
        self.assertIn("return_dates", form.errors)

    def test_prescription_admin_rejects_return_dates_without_iso_date(self):
        model_admin = admin.site._registry[Prescription]
        prescription = Prescription.objects.create(
            ccmdd_prescription_id="rx-1",
            date_created=datetime(2026, 5, 1, tzinfo=timezone.utc),
            date_updated=datetime(2026, 5, 1, tzinfo=timezone.utc),
            facility_id=1,
            patient_id="patient-1",
            patient_phone="0820000001",
            department_id=1,
            return_dates=[],
            payload={},
        )
        request = RequestFactory().post("/admin/synch/prescription/1/change/")
        form_class = model_admin.get_form(request, obj=prescription)
        form = form_class(
            data={
                "ccmdd_prescription_id": prescription.ccmdd_prescription_id,
                "date_created_0": "2026-05-01",
                "date_created_1": "00:00:00",
                "date_updated_0": "2026-05-01",
                "date_updated_1": "00:00:00",
                "facility_id": "1",
                "patient_id": "patient-1",
                "patient_phone": "0820000001",
                "department_id": "1",
                "return_dates": '[{"return_date": "04-06-2026"}]',
                "payload": "{}",
            },
            instance=prescription,
        )

        self.assertFalse(form.is_valid())
        self.assertIn("return_dates", form.errors)


class FacilityAdminTests(TestCase):
    def test_facility_model_is_registered_in_admin(self):
        self.assertIn(Facility, admin.site._registry)

    def test_facility_admin_lists_basic_sync_fields(self):
        model_admin = admin.site._registry[Facility]

        self.assertEqual(
            model_admin.list_display,
            ("ccmdd_facility_id", "name", "telephone"),
        )
        self.assertEqual(model_admin.search_fields, ("ccmdd_facility_id", "name"))
