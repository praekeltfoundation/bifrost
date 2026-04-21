from django.contrib import admin

from synch.models import Facility, Patient, Prescription


@admin.register(Patient)
class PatientAdmin(admin.ModelAdmin):
    list_display = ("ccmdd_patient_id", "date_created", "date_updated")
    search_fields = ("ccmdd_patient_id",)
    readonly_fields = ("payload",)


@admin.register(Prescription)
class PrescriptionAdmin(admin.ModelAdmin):
    list_display = (
        "ccmdd_prescription_id",
        "patient_id",
        "date_created",
        "date_updated",
    )
    search_fields = ("ccmdd_prescription_id", "patient_id")
    readonly_fields = ("return_dates", "payload")


@admin.register(Facility)
class FacilityAdmin(admin.ModelAdmin):
    list_display = ("ccmdd_facility_id", "name", "telephone", "address_1", "address_2")
    search_fields = ("ccmdd_facility_id", "name")
    readonly_fields = ("payload",)
