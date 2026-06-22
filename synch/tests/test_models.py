from __future__ import annotations

from datetime import date, datetime, timezone

from django.test import TestCase

from synch.models import Facility, Patient, Prescription


class PatientModelTests(TestCase):
    def test_string_representation_uses_patient_id(self):
        patient = Patient.objects.create(
            ccmdd_patient_id="90653BC3-DF69-E611-9D09-20689D5CEDFC",
            date_created=datetime(2016, 4, 8, 12, 48, 15, tzinfo=timezone.utc),
            date_updated=datetime(2016, 4, 29, 11, 25, 28, tzinfo=timezone.utc),
            payload={"surname": "wer"},
        )

        self.assertEqual(str(patient), patient.ccmdd_patient_id)

    def test_messaging_phone_number_uses_most_recent_valid_phone(self):
        patient = Patient.objects.create(
            ccmdd_patient_id="patient-1",
            date_created=datetime(2026, 4, 1, 0, 0, 1, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 1, 0, 0, 1, tzinfo=timezone.utc),
            payload={},
        )
        Prescription.objects.create(
            ccmdd_prescription_id="rx-old-valid",
            date_created=datetime(2026, 4, 1, 1, 0, 0, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 1, 1, 0, 0, tzinfo=timezone.utc),
            facility_id=1,
            patient_id=patient.ccmdd_patient_id,
            patient_phone="0820000001",
            department_id=1,
            return_dates=[],
            payload={},
        )
        Prescription.objects.create(
            ccmdd_prescription_id="rx-new-invalid",
            date_created=datetime(2026, 4, 2, 1, 0, 0, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 2, 1, 0, 0, tzinfo=timezone.utc),
            facility_id=1,
            patient_id=patient.ccmdd_patient_id,
            patient_phone="not-a-phone-number",
            department_id=1,
            return_dates=[],
            payload={},
        )

        self.assertEqual(patient.messaging_phone_number, "+27820000001")

    def test_tracked_appointment_skips_unusable_facilities_and_breaks_ties_by_recency(
        self,
    ):
        patient = Patient.objects.create(
            ccmdd_patient_id="patient-1",
            date_created=datetime(2026, 4, 1, 0, 0, 1, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 1, 0, 0, 1, tzinfo=timezone.utc),
            payload={},
        )
        Facility.objects.create(
            ccmdd_facility_id=2,
            name="",
            latitude="1",
            longitude="2",
            telephone="",
            address_1="",
            address_2="",
            payload={},
        )
        preferred_facility = Facility.objects.create(
            ccmdd_facility_id=3,
            name="Clinic B",
            latitude="",
            longitude="",
            telephone="",
            address_1="",
            address_2="",
            payload={},
        )
        Prescription.objects.create(
            ccmdd_prescription_id="rx-unusable-facility",
            date_created=datetime(2026, 4, 1, 1, 0, 0, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 1, 1, 0, 0, tzinfo=timezone.utc),
            facility_id=2,
            patient_id=patient.ccmdd_patient_id,
            patient_phone="0820000001",
            department_id=1,
            return_dates=[{"return_date": "2026-04-21"}],
            payload={},
        )
        Prescription.objects.create(
            ccmdd_prescription_id="rx-same-day-older",
            date_created=datetime(2026, 4, 2, 1, 0, 0, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 2, 1, 0, 0, tzinfo=timezone.utc),
            facility_id=999,
            patient_id=patient.ccmdd_patient_id,
            patient_phone="0820000002",
            department_id=1,
            return_dates=[{"return_date": "2026-04-22"}],
            payload={},
        )
        chosen_prescription = Prescription.objects.create(
            ccmdd_prescription_id="rx-same-day-newer",
            date_created=datetime(2026, 4, 3, 1, 0, 0, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 3, 1, 0, 0, tzinfo=timezone.utc),
            facility_id=3,
            patient_id=patient.ccmdd_patient_id,
            patient_phone="0820000003",
            department_id=1,
            return_dates=[{"return_date": "2026-04-22"}],
            payload={},
        )

        appointment = patient.get_tracked_appointment(today=date(2026, 4, 21))

        self.assertIsNotNone(appointment)
        if appointment is None:
            self.fail("Expected an tracked appointment")
        self.assertEqual(appointment.date, date(2026, 4, 22))
        self.assertEqual(appointment.prescription, chosen_prescription)
        self.assertEqual(appointment.facility, preferred_facility)

    def test_tracked_appointment_uses_single_facility_query_for_patient(self):
        patient = Patient.objects.create(
            ccmdd_patient_id="patient-1",
            date_created=datetime(2026, 4, 1, 0, 0, 1, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 1, 0, 0, 1, tzinfo=timezone.utc),
            payload={},
        )
        Facility.objects.create(
            ccmdd_facility_id=1,
            name="Clinic A",
            latitude="",
            longitude="",
            telephone="",
            address_1="",
            address_2="",
            payload={},
        )
        Facility.objects.create(
            ccmdd_facility_id=2,
            name="Clinic B",
            latitude="",
            longitude="",
            telephone="",
            address_1="",
            address_2="",
            payload={},
        )
        for prescription_id, facility_id in (("rx-1", 1), ("rx-2", 2), ("rx-3", 999)):
            Prescription.objects.create(
                ccmdd_prescription_id=prescription_id,
                date_created=datetime(2026, 4, 2, 1, 0, 0, tzinfo=timezone.utc),
                date_updated=datetime(2026, 4, 2, 1, 0, 0, tzinfo=timezone.utc),
                facility_id=facility_id,
                patient_id=patient.ccmdd_patient_id,
                patient_phone="0820000001",
                department_id=1,
                return_dates=[{"return_date": "2026-04-22"}],
                payload={},
            )

        with self.assertNumQueries(2):
            appointment = patient.get_tracked_appointment(today=date(2026, 4, 21))

        self.assertIsNotNone(appointment)

    def test_tracked_appointment_stays_active_until_window_ends(
        self,
    ):
        patient = Patient.objects.create(
            ccmdd_patient_id="patient-1",
            date_created=datetime(2026, 4, 1, 0, 0, 1, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 1, 0, 0, 1, tzinfo=timezone.utc),
            payload={},
        )
        facility = Facility.objects.create(
            ccmdd_facility_id=1,
            name="Clinic A",
            latitude="",
            longitude="",
            telephone="",
            address_1="",
            address_2="",
            payload={},
        )
        prescription = Prescription.objects.create(
            ccmdd_prescription_id="rx-appointment",
            date_created=datetime(2026, 4, 1, 1, 0, 0, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 1, 1, 0, 0, tzinfo=timezone.utc),
            facility_id=1,
            patient_id=patient.ccmdd_patient_id,
            patient_phone="0820000001",
            department_id=1,
            return_dates=[{"return_date": "2026-04-22"}],
            payload={},
        )

        appointment = patient.get_tracked_appointment(today=date(2026, 5, 1))

        if appointment is None:
            self.fail("Expected a tracked appointment")
        self.assertEqual(appointment.date, date(2026, 4, 22))
        self.assertEqual(appointment.prescription, prescription)
        self.assertEqual(appointment.facility, facility)

    def test_related_prescription_resolves_tracked_appointment_and_tracks_next_date(
        self,
    ):
        patient = Patient.objects.create(
            ccmdd_patient_id="patient-1",
            date_created=datetime(2026, 4, 1, 0, 0, 1, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 1, 0, 0, 1, tzinfo=timezone.utc),
            payload={},
        )
        Facility.objects.create(
            ccmdd_facility_id=1,
            name="Clinic A",
            latitude="",
            longitude="",
            telephone="",
            address_1="",
            address_2="",
            payload={},
        )
        next_facility = Facility.objects.create(
            ccmdd_facility_id=2,
            name="Clinic B",
            latitude="",
            longitude="",
            telephone="",
            address_1="",
            address_2="",
            payload={},
        )
        Prescription.objects.create(
            ccmdd_prescription_id="rx-previous",
            date_created=datetime(2026, 4, 1, 1, 0, 0, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 1, 1, 0, 0, tzinfo=timezone.utc),
            facility_id=1,
            patient_id=patient.ccmdd_patient_id,
            patient_phone="0820000001",
            department_id=1,
            return_dates=[{"return_date": "2026-04-22"}],
            payload={},
        )
        next_prescription = Prescription.objects.create(
            ccmdd_prescription_id="rx-next",
            date_created=datetime(2026, 4, 23, 1, 0, 0, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 23, 1, 0, 0, tzinfo=timezone.utc),
            facility_id=2,
            patient_id=patient.ccmdd_patient_id,
            patient_phone="0820000001",
            department_id=1,
            return_dates=[{"return_date": "2026-05-21"}],
            payload={},
        )

        appointment = patient.get_tracked_appointment(today=date(2026, 4, 24))

        if appointment is None:
            self.fail("Expected a tracked appointment")
        self.assertEqual(appointment.date, date(2026, 5, 21))
        self.assertEqual(appointment.prescription, next_prescription)
        self.assertEqual(appointment.facility, next_facility)

    def test_prescription_does_not_resolve_its_own_tracked_appointment(self):
        patient = Patient.objects.create(
            ccmdd_patient_id="patient-1",
            date_created=datetime(2026, 4, 1, 0, 0, 1, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 1, 0, 0, 1, tzinfo=timezone.utc),
            payload={},
        )
        facility = Facility.objects.create(
            ccmdd_facility_id=1,
            name="Clinic A",
            latitude="",
            longitude="",
            telephone="",
            address_1="",
            address_2="",
            payload={},
        )
        prescription = Prescription.objects.create(
            ccmdd_prescription_id="rx-short-return",
            date_created=datetime(2026, 4, 20, 1, 0, 0, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 20, 1, 0, 0, tzinfo=timezone.utc),
            facility_id=1,
            patient_id=patient.ccmdd_patient_id,
            patient_phone="0820000001",
            department_id=1,
            return_dates=[{"return_date": "2026-05-01"}],
            payload={},
        )

        appointment = patient.get_tracked_appointment(today=date(2026, 4, 21))

        if appointment is None:
            self.fail("Expected a tracked appointment")
        self.assertEqual(appointment.date, date(2026, 5, 1))
        self.assertEqual(appointment.prescription, prescription)
        self.assertEqual(appointment.facility, facility)

    def test_missed_appointment_is_skipped_for_next_unresolved_appointment(self):
        patient = Patient.objects.create(
            ccmdd_patient_id="patient-1",
            date_created=datetime(2026, 4, 1, 0, 0, 1, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 1, 0, 0, 1, tzinfo=timezone.utc),
            payload={},
        )
        Facility.objects.create(
            ccmdd_facility_id=1,
            name="Clinic A",
            latitude="",
            longitude="",
            telephone="",
            address_1="",
            address_2="",
            payload={},
        )
        next_facility = Facility.objects.create(
            ccmdd_facility_id=2,
            name="Clinic B",
            latitude="",
            longitude="",
            telephone="",
            address_1="",
            address_2="",
            payload={},
        )
        Prescription.objects.create(
            ccmdd_prescription_id="rx-missed",
            date_created=datetime(2026, 4, 1, 1, 0, 0, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 1, 1, 0, 0, tzinfo=timezone.utc),
            facility_id=1,
            patient_id=patient.ccmdd_patient_id,
            patient_phone="0820000001",
            department_id=1,
            return_dates=[{"return_date": "2026-04-22"}],
            payload={},
        )
        next_prescription = Prescription.objects.create(
            ccmdd_prescription_id="rx-next",
            date_created=datetime(2026, 6, 28, 1, 0, 0, tzinfo=timezone.utc),
            date_updated=datetime(2026, 6, 28, 1, 0, 0, tzinfo=timezone.utc),
            facility_id=2,
            patient_id=patient.ccmdd_patient_id,
            patient_phone="0820000001",
            department_id=1,
            return_dates=[{"return_date": "2026-07-15"}],
            payload={},
        )

        appointment = patient.get_tracked_appointment(today=date(2026, 6, 18))

        if appointment is None:
            self.fail("Expected a tracked appointment")
        self.assertEqual(appointment.date, date(2026, 7, 15))
        self.assertEqual(appointment.prescription, next_prescription)
        self.assertEqual(appointment.facility, next_facility)

    def test_turn_sync_details_fall_back_to_latest_usable_facility(
        self,
    ):
        patient = Patient.objects.create(
            ccmdd_patient_id="patient-1",
            date_created=datetime(2026, 4, 1, 0, 0, 1, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 1, 0, 0, 1, tzinfo=timezone.utc),
            payload={},
        )
        Facility.objects.create(
            ccmdd_facility_id=1,
            name="Clinic A",
            latitude="-26.1",
            longitude="28.1",
            telephone="",
            address_1="",
            address_2="",
            payload={},
        )
        latest_facility = Facility.objects.create(
            ccmdd_facility_id=2,
            name="Clinic B",
            latitude="-26.2",
            longitude="28.2",
            telephone="",
            address_1="",
            address_2="",
            payload={},
        )
        Prescription.objects.create(
            ccmdd_prescription_id="rx-old",
            date_created=datetime(2026, 4, 2, 1, 0, 0, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 2, 1, 0, 0, tzinfo=timezone.utc),
            facility_id=1,
            patient_id=patient.ccmdd_patient_id,
            patient_phone="0820000001",
            department_id=1,
            return_dates=[{"return_date": "2026-02-20"}],
            payload={},
        )
        Prescription.objects.create(
            ccmdd_prescription_id="rx-new",
            date_created=datetime(2026, 4, 3, 1, 0, 0, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 3, 1, 0, 0, tzinfo=timezone.utc),
            facility_id=2,
            patient_id=patient.ccmdd_patient_id,
            patient_phone="0820000002",
            department_id=1,
            return_dates=[],
            payload={},
        )

        sync_details = patient.get_turn_sync_details(today=date(2026, 4, 21))

        self.assertEqual(sync_details.messaging_phone_number, "+27820000002")
        self.assertIsNone(sync_details.tracked_appointment)
        self.assertEqual(sync_details.messaging_facility, latest_facility)

    def test_turn_sync_details_fall_back_when_future_date_lacks_usable_facility(
        self,
    ):
        patient = Patient.objects.create(
            ccmdd_patient_id="patient-1",
            date_created=datetime(2026, 4, 1, 0, 0, 1, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 1, 0, 0, 1, tzinfo=timezone.utc),
            payload={},
        )
        fallback_facility = Facility.objects.create(
            ccmdd_facility_id=1,
            name="Clinic A",
            latitude="",
            longitude="",
            telephone="",
            address_1="",
            address_2="",
            payload={},
        )
        Prescription.objects.create(
            ccmdd_prescription_id="rx-fallback",
            date_created=datetime(2026, 4, 2, 1, 0, 0, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 2, 1, 0, 0, tzinfo=timezone.utc),
            facility_id=1,
            patient_id=patient.ccmdd_patient_id,
            patient_phone="0820000001",
            department_id=1,
            return_dates=[],
            payload={},
        )
        Prescription.objects.create(
            ccmdd_prescription_id="rx-future-missing-facility",
            date_created=datetime(2026, 4, 3, 1, 0, 0, tzinfo=timezone.utc),
            date_updated=datetime(2026, 4, 3, 1, 0, 0, tzinfo=timezone.utc),
            facility_id=999,
            patient_id=patient.ccmdd_patient_id,
            patient_phone="0820000002",
            department_id=1,
            return_dates=[{"return_date": "2026-04-22"}],
            payload={},
        )

        sync_details = patient.get_turn_sync_details(today=date(2026, 4, 21))

        self.assertEqual(sync_details.messaging_phone_number, "+27820000002")
        self.assertIsNone(sync_details.tracked_appointment)
        self.assertEqual(sync_details.messaging_facility, fallback_facility)


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


class FacilityModelTests(TestCase):
    def test_string_representation_uses_facility_name(self):
        facility = Facility.objects.create(
            ccmdd_facility_id=110533,
            name="Addo Clinic",
            latitude="-33.5422",
            longitude="25.6908",
            telephone="0123456789",
            address_1="Main Road",
            address_2="Addo",
            payload={"classification": "Clinic"},
        )

        self.assertEqual(str(facility), facility.name)

    def test_is_usable_for_messaging_requires_non_blank_name(self):
        blank_name_facility = Facility.objects.create(
            ccmdd_facility_id=110534,
            name="",
            latitude="-33.5422",
            longitude="25.6908",
            telephone="0123456789",
            address_1="Main Road",
            address_2="Addo",
            payload={"classification": "Clinic"},
        )

        self.assertFalse(blank_name_facility.is_usable_for_messaging)
