from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, cast
from unittest.mock import patch

from django.contrib import admin
from django.db import models
from django.test import RequestFactory, TestCase

from edrweb.models import EDRWebPatient


class EDRWebPatientAdminTests(TestCase):
    def test_edrweb_patient_model_is_registered_in_admin(self):
        self.assertIn(EDRWebPatient, admin.site._registry)

    def test_edrweb_patient_admin_lists_basic_sync_fields(self):
        model_admin = admin.site._registry[EDRWebPatient]

        self.assertEqual(
            model_admin.list_display,
            (
                "patient_id",
                "phone_number",
                "updated_at",
                "is_active",
                "feed_removed_at",
            ),
        )
        self.assertEqual(model_admin.search_fields, ("patient_id", "phone_number"))

    def test_edrweb_patient_admin_allows_expected_manual_edits(self):
        model_admin = admin.site._registry[EDRWebPatient]
        patient = self._create_patient()
        request = RequestFactory().get("/admin/edrweb/edrwebpatient/1/change/")

        form = model_admin.get_form(request, obj=patient)(instance=patient)

        self.assertIn("patient_id", form.fields)
        self.assertIn("phone_number", form.fields)
        self.assertIn("updated_at", form.fields)
        self.assertIn("is_active", form.fields)
        self.assertIn("appointments", form.fields)
        self.assertNotIn("feed_removed_at", form.fields)
        self.assertNotIn("payload", form.fields)
        self.assertIn("feed_removed_at", model_admin.readonly_fields)
        self.assertIn("payload", model_admin.readonly_fields)

    def test_edrweb_patient_model_fields_document_manual_admin_use(self):
        documented_fields = (
            "patient_id",
            "phone_number",
            "updated_at",
            "is_active",
            "feed_removed_at",
            "appointments",
            "payload",
        )

        for field_name in documented_fields:
            with self.subTest(field_name=field_name):
                field = cast(
                    "models.Field[Any, Any]",
                    EDRWebPatient._meta.get_field(field_name),
                )

                self.assertTrue(field.help_text)

    def test_edrweb_patient_admin_accepts_upstream_shaped_appointments(self):
        patient = self._create_patient()
        form = self._build_form(
            patient,
            {
                "appointments": (
                    '[{"AppointmentDate": "2026-06-20", '
                    '"Facility": {"FacilityName": "WC BLUE DOWNS CLINIC", '
                    '"Latitude": -33.9744, "Longitude": 18.7032}}]'
                ),
            },
        )

        self.assertTrue(form.is_valid(), form.errors)

    def test_edrweb_patient_admin_rejects_non_list_appointments(self):
        patient = self._create_patient()
        form = self._build_form(patient, {"appointments": '{"AppointmentDate": "x"}'})

        self.assertFalse(form.is_valid())
        self.assertIn("appointments", form.errors)

    def test_edrweb_patient_admin_rejects_appointment_without_iso_date(self):
        patient = self._create_patient()
        form = self._build_form(
            patient,
            {"appointments": '[{"AppointmentDate": "20-06-2026"}]'},
        )

        self.assertFalse(form.is_valid())
        self.assertIn("appointments", form.errors)

    def test_edrweb_patient_admin_rejects_appointment_with_unknown_keys(self):
        patient = self._create_patient()
        form = self._build_form(
            patient,
            {
                "appointments": (
                    '[{"AppointmentDate": "2026-06-20", "ReminderType": "SMS"}]'
                ),
            },
        )

        self.assertFalse(form.is_valid())
        self.assertIn("appointments", form.errors)

    def test_edrweb_patient_admin_rejects_invalid_facility_shape(self):
        patient = self._create_patient()
        form = self._build_form(
            patient,
            {
                "appointments": (
                    '[{"AppointmentDate": "2026-06-20", '
                    '"Facility": {"FacilityName": "WC BLUE DOWNS CLINIC", '
                    '"Latitude": "not-a-number"}}]'
                ),
            },
        )

        self.assertFalse(form.is_valid())
        self.assertIn("appointments", form.errors)

    def test_edrweb_patient_admin_rejects_future_updated_at(self):
        patient = self._create_patient()
        form = self._build_form(
            patient,
            {
                "updated_at_0": "9999-01-01",
                "updated_at_1": "00:00:00",
            },
        )

        self.assertFalse(form.is_valid())
        self.assertIn("updated_at", form.errors)

    def test_edrweb_patient_admin_sets_feed_removed_at_when_deactivated(self):
        patient = self._create_patient()
        form = self._build_form(patient, {"is_active": ""})
        self.assertTrue(form.is_valid(), form.errors)
        changed_patient = form.save(commit=False)
        request = RequestFactory().post("/admin/edrweb/edrwebpatient/1/change/")
        model_admin = admin.site._registry[EDRWebPatient]
        removal_time = datetime(2026, 6, 10, 10, 0, tzinfo=timezone.utc)

        with patch("edrweb.admin.django_timezone.now", return_value=removal_time):
            model_admin.save_model(request, changed_patient, form, change=True)

        patient.refresh_from_db()
        self.assertFalse(patient.is_active)
        self.assertEqual(patient.feed_removed_at, removal_time)

    def test_edrweb_patient_admin_sets_feed_removed_at_when_created_inactive(self):
        form = self._build_add_form({"is_active": ""})
        self.assertTrue(form.is_valid(), form.errors)
        patient = form.save(commit=False)
        request = RequestFactory().post("/admin/edrweb/edrwebpatient/add/")
        model_admin = admin.site._registry[EDRWebPatient]
        removal_time = datetime(2026, 6, 10, 10, 0, tzinfo=timezone.utc)

        with patch("edrweb.admin.django_timezone.now", return_value=removal_time):
            model_admin.save_model(request, patient, form, change=False)

        patient.refresh_from_db()
        self.assertFalse(patient.is_active)
        self.assertEqual(patient.feed_removed_at, removal_time)

    def test_edrweb_patient_admin_keeps_existing_feed_removed_at_when_still_inactive(
        self,
    ):
        removal_time = datetime(2026, 6, 9, 10, 0, tzinfo=timezone.utc)
        patient = self._create_patient(is_active=False, feed_removed_at=removal_time)
        form = self._build_form(patient, {"is_active": ""})
        self.assertTrue(form.is_valid(), form.errors)
        changed_patient = form.save(commit=False)
        request = RequestFactory().post("/admin/edrweb/edrwebpatient/1/change/")
        model_admin = admin.site._registry[EDRWebPatient]

        model_admin.save_model(request, changed_patient, form, change=True)

        patient.refresh_from_db()
        self.assertFalse(patient.is_active)
        self.assertEqual(patient.feed_removed_at, removal_time)

    def test_edrweb_patient_admin_clears_feed_removed_at_when_reactivated(self):
        patient = self._create_patient(
            is_active=False,
            feed_removed_at=datetime(2026, 6, 9, 10, 0, tzinfo=timezone.utc),
        )
        form = self._build_form(patient, {"is_active": "on"})
        self.assertTrue(form.is_valid(), form.errors)
        changed_patient = form.save(commit=False)
        request = RequestFactory().post("/admin/edrweb/edrwebpatient/1/change/")
        model_admin = admin.site._registry[EDRWebPatient]

        model_admin.save_model(request, changed_patient, form, change=True)

        patient.refresh_from_db()
        self.assertTrue(patient.is_active)
        self.assertIsNone(patient.feed_removed_at)

    def _create_patient(
        self,
        *,
        is_active: bool = True,
        feed_removed_at: datetime | None = None,
    ) -> EDRWebPatient:
        return EDRWebPatient.objects.create(
            patient_id="edrweb-patient-1",
            phone_number="+27721234567",
            updated_at=datetime(2026, 5, 30, 12, 22, tzinfo=timezone.utc),
            is_active=is_active,
            feed_removed_at=feed_removed_at,
            appointments=[],
            payload={"FirstName": "Test"},
        )

    def _build_form(
        self,
        patient: EDRWebPatient,
        data_overrides: dict[str, str],
    ):
        request = RequestFactory().post("/admin/edrweb/edrwebpatient/1/change/")
        model_admin = admin.site._registry[EDRWebPatient]
        form_class = model_admin.get_form(request, obj=patient)
        data = {
            "patient_id": patient.patient_id,
            "phone_number": patient.phone_number,
            "updated_at_0": "2026-05-30",
            "updated_at_1": "12:22:00",
            "is_active": "on" if patient.is_active else "",
            "appointments": "[]",
        }
        data.update(data_overrides)
        return form_class(data=data, instance=patient)

    def _build_add_form(self, data_overrides: dict[str, str]):
        request = RequestFactory().post("/admin/edrweb/edrwebpatient/add/")
        model_admin = admin.site._registry[EDRWebPatient]
        form_class = model_admin.get_form(request)
        data = {
            "patient_id": "new-edrweb-patient",
            "phone_number": "+27721234567",
            "updated_at_0": "2026-05-30",
            "updated_at_1": "12:22:00",
            "is_active": "on",
            "appointments": "[]",
        }
        data.update(data_overrides)
        return form_class(data=data)
