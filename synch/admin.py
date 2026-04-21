from django.contrib import admin

from synch.models import Patient, Prescription


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
