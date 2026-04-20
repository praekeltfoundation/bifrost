from django.contrib import admin

from synch.models import Patient


@admin.register(Patient)
class PatientAdmin(admin.ModelAdmin):
    list_display = ("ccmdd_patient_id", "date_created", "date_updated")
    search_fields = ("ccmdd_patient_id",)
    readonly_fields = ("payload",)
