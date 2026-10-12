from __future__ import annotations

import os
import resource
from collections import defaultdict
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from time import perf_counter
from typing import Any
from unittest.mock import patch

from django.core.management.base import BaseCommand, CommandError
from django.db import connection, transaction
from django.utils import timezone as django_timezone

import synch.tasks as synch_tasks
from lock.models import Lock
from synch.models import Facility, Patient, Prescription

BENCHMARK_PREFIX = "benchmark-sync-all"
BENCHMARK_DATE = datetime(2026, 6, 24, 6, 0, 0, tzinfo=timezone.utc)
FACILITY_ID_OFFSET = 90_000_000


@dataclass(frozen=True)
class BenchmarkScale:
    facilities: int
    patients: int
    prescriptions: int
    appointment_updates: int
    new_patients: int
    patient_updates: int
    prescription_updates: int
    phone_changes: int


@dataclass
class QueryGroupProfile:
    count: int = 0
    seconds: float = 0.0


@dataclass
class StepProfile:
    calls: int = 0
    seconds: float = 0.0
    query_count: int = 0
    query_seconds: float = 0.0
    query_groups: dict[str, QueryGroupProfile] | None = None
    rss_before_mib: float = 0.0
    rss_after_mib: float = 0.0
    rss_peak_mib: float = 0.0


class BenchmarkCCMDDClient:
    def __init__(self, scale: BenchmarkScale) -> None:
        self.scale = scale

    def iter_facilities(self) -> Iterator[dict[str, Any]]:
        for index in range(self.scale.facilities):
            yield _facility_record(index)

    def iter_limited_prescriptions(
        self,
        date_updated: datetime | None = None,
    ) -> Iterator[dict[str, Any]]:
        del date_updated
        for index in range(self.scale.prescription_updates):
            yield _prescription_record(
                index=self.scale.prescriptions + index,
                patient_index=index % self.scale.appointment_updates,
                facility_count=self.scale.facilities,
                date_updated=BENCHMARK_DATE + timedelta(minutes=10),
            )

    def iter_limited_patients(
        self,
        date_updated: datetime | None = None,
        prescription_date_updated: datetime | None = None,
    ) -> Iterator[dict[str, Any]]:
        if date_updated is not None:
            for index in range(self.scale.patient_updates):
                yield _patient_record(index, BENCHMARK_DATE + timedelta(minutes=20))
            return

        if prescription_date_updated is not None:
            start = self.scale.patient_updates
            stop = start + self.scale.prescription_updates
            for index in range(start, stop):
                yield _patient_record(index, BENCHMARK_DATE + timedelta(minutes=30))


class BenchmarkTurnClient:
    def __init__(self) -> None:
        self.import_counts: list[int] = []
        self.import_fieldsets: list[tuple[str, ...]] = []

    def import_contacts(self, rows: list[dict[str, object]]) -> list[dict[str, str]]:
        self.import_counts.append(len(rows))
        if rows:
            self.import_fieldsets.append(tuple(rows[0]))
        return []


class Command(BaseCommand):
    help = "Benchmark synch.tasks.sync_all with mocked external CCMDD and Turn calls."

    def add_arguments(self, parser) -> None:
        parser.add_argument("--facilities", type=int, default=10732)
        parser.add_argument("--patients", type=int, default=75132)
        parser.add_argument("--prescriptions", type=int, default=82791)
        parser.add_argument("--appointment-updates", type=int, default=4)
        parser.add_argument("--new-patients", type=int, default=4)
        parser.add_argument("--patient-updates", type=int, default=5)
        parser.add_argument("--prescription-updates", type=int, default=5)
        parser.add_argument("--phone-changes", type=int, default=0)
        mode = parser.add_mutually_exclusive_group()
        mode.add_argument(
            "--seed-only",
            action="store_true",
            help=(
                "Seed data and exit, so a later run can measure memory "
                "in a fresh process."
            ),
        )
        mode.add_argument(
            "--run-only",
            action="store_true",
            help="Run against previously seeded benchmark data without seeding again.",
        )
        parser.add_argument(
            "--allow-existing-data",
            action="store_true",
            help="Run even when non-benchmark synch data exists.",
        )
        parser.add_argument(
            "--cleanup",
            action="store_true",
            help="Delete benchmark rows after the run.",
        )

    def handle(self, *_args, **options) -> None:
        scale = BenchmarkScale(
            facilities=options["facilities"],
            patients=options["patients"],
            prescriptions=options["prescriptions"],
            appointment_updates=options["appointment_updates"],
            new_patients=options["new_patients"],
            patient_updates=options["patient_updates"],
            prescription_updates=options["prescription_updates"],
            phone_changes=options["phone_changes"],
        )
        self._validate_scale(scale)

        if not options["allow_existing_data"]:
            self._assert_no_non_benchmark_data()

        seed_seconds = 0.0
        if options["run_only"]:
            self._assert_benchmark_scale(scale)
        else:
            self._delete_benchmark_data()
            self.stdout.write("Seeding benchmark data...")
            seed_seconds = self._time(lambda: self._seed_benchmark_data(scale))
            if options["seed_only"]:
                return

        step_profiles: dict[str, StepProfile] = {}
        turn_client = BenchmarkTurnClient()
        total_query_count = 0

        def count_query(execute, sql, params, many, context):
            nonlocal total_query_count
            total_query_count += 1
            return execute(sql, params, many, context)

        self.stdout.write("Running sync_all benchmark...")
        rss_baseline_mib = _rss_mib()
        with (
            patch(
                "synch.tasks.CCMDDAPIClient", return_value=BenchmarkCCMDDClient(scale)
            ),
            patch("synch.tasks.TurnAPIClient", return_value=turn_client),
            self._patch_profiled_step("sync_facilities", step_profiles),
            self._patch_profiled_step("sync_prescriptions", step_profiles),
            self._patch_profiled_step("sync_patients", step_profiles),
            self._patch_profiled_step(
                "build_patient_messaging_snapshot", step_profiles
            ),
            self._patch_profiled_step(
                "sync_appointment_dates_to_turn",
                step_profiles,
            ),
            self._patch_profiled_step("sync_new_patients_to_turn", step_profiles),
            self._patch_profiled_step(
                "sync_changed_patient_phone_numbers_to_turn",
                step_profiles,
            ),
            connection.execute_wrapper(count_query),
        ):
            total_seconds = self._time(synch_tasks.sync_all)
        rss_after_sync_mib = _rss_mib()

        self._write_report(
            seed_seconds=seed_seconds,
            total_seconds=total_seconds,
            total_query_count=total_query_count,
            step_profiles=step_profiles,
            turn_client=turn_client,
            scale=scale,
            rss_baseline_mib=rss_baseline_mib,
            rss_after_sync_mib=rss_after_sync_mib,
        )

        if options["cleanup"]:
            self._delete_benchmark_data()

    def _validate_scale(self, scale: BenchmarkScale) -> None:
        positive_fields = {
            "facilities": scale.facilities,
            "patients": scale.patients,
            "prescriptions": scale.prescriptions,
        }
        for name, value in positive_fields.items():
            if value < 1:
                raise CommandError(f"--{name.replace('_', '-')} must be at least 1.")

        bounded_fields = {
            "appointment-updates": scale.appointment_updates,
            "new-patients": scale.new_patients,
            "patient-updates": scale.patient_updates,
            "prescription-updates": scale.prescription_updates,
            "phone-changes": scale.phone_changes,
        }
        for name, value in bounded_fields.items():
            if value < 0 or value > scale.patients:
                raise CommandError(f"--{name} must be between 0 and --patients.")

        if scale.appointment_updates < 1:
            raise CommandError("--appointment-updates must be at least 1.")
        if scale.appointment_updates < scale.new_patients:
            raise CommandError("--appointment-updates must be at least --new-patients.")
        if scale.appointment_updates < scale.new_patients + scale.phone_changes:
            raise CommandError(
                "--appointment-updates must cover --new-patients plus --phone-changes."
            )
        if scale.patient_updates + scale.prescription_updates > scale.patients:
            raise CommandError(
                "--patient-updates plus --prescription-updates must not exceed "
                "--patients."
            )

    def _assert_no_non_benchmark_data(self) -> None:
        has_non_benchmark_data = (
            Facility.objects.count()
            != Facility.objects.filter(payload__benchmark_sync_all=True).count()
            or Patient.objects.count()
            != Patient.objects.filter(payload__benchmark_sync_all=True).count()
            or Prescription.objects.count()
            != Prescription.objects.filter(payload__benchmark_sync_all=True).count()
        )
        if has_non_benchmark_data:
            raise CommandError(
                "Non-benchmark synch data exists. Use a dedicated benchmark database "
                "or pass --allow-existing-data if mixed data is intentional."
            )

    def _delete_benchmark_data(self) -> None:
        Lock.objects.filter(key=synch_tasks.CCMDD_SYNC_LOCK_KEY).delete()
        Prescription.objects.filter(payload__benchmark_sync_all=True).delete()
        Patient.objects.filter(payload__benchmark_sync_all=True).delete()
        Facility.objects.filter(payload__benchmark_sync_all=True).delete()

    def _assert_benchmark_scale(self, scale: BenchmarkScale) -> None:
        actual = (
            Facility.objects.filter(payload__benchmark_sync_all=True).count(),
            Patient.objects.filter(payload__benchmark_sync_all=True).count(),
            Prescription.objects.filter(payload__benchmark_sync_all=True).count(),
        )
        expected = (scale.facilities, scale.patients, scale.prescriptions)
        if actual != expected:
            raise CommandError(
                f"Seeded counts {actual} do not match requested scale {expected}."
            )

    def _seed_benchmark_data(self, scale: BenchmarkScale) -> None:
        with transaction.atomic():
            Facility.objects.bulk_create(
                [
                    Facility(
                        ccmdd_facility_id=_facility_id(index),
                        name=f"Benchmark Clinic {index + 1}",
                        latitude="-26.2041",
                        longitude="28.0473",
                        telephone="",
                        address_1="Benchmark Street",
                        address_2="",
                        payload=_facility_payload(),
                    )
                    for index in range(scale.facilities)
                ],
                batch_size=1000,
            )
            Patient.objects.bulk_create(
                [
                    Patient(
                        ccmdd_patient_id=_patient_id(index),
                        date_created=BENCHMARK_DATE - timedelta(days=90),
                        date_updated=BENCHMARK_DATE - timedelta(days=1),
                        invite_sent=index >= scale.new_patients,
                        active_messaging_phone_number=(
                            _active_phone(index, scale)
                            if index >= scale.new_patients
                            else ""
                        ),
                        **_patient_context(index, scale),
                        payload=_patient_payload(),
                    )
                    for index in range(scale.patients)
                ],
                batch_size=1000,
            )
            Prescription.objects.bulk_create(
                [
                    Prescription(
                        ccmdd_prescription_id=_prescription_id(index),
                        date_created=BENCHMARK_DATE
                        - timedelta(days=30)
                        + timedelta(seconds=index),
                        date_updated=BENCHMARK_DATE - timedelta(days=1),
                        facility_id=_facility_id(index % scale.facilities),
                        patient_id=_patient_id(index % scale.patients),
                        patient_phone=_current_phone(index % scale.patients),
                        department_id=1,
                        return_dates=_return_dates(),
                        payload=_prescription_payload(),
                    )
                    for index in range(scale.prescriptions)
                ],
                batch_size=1000,
            )

    def _patch_profiled_step(self, name: str, profiles: dict[str, StepProfile]):
        original = getattr(synch_tasks, name)

        def profiled_step(*args, **kwargs):
            query_groups: dict[str, QueryGroupProfile] = defaultdict(QueryGroupProfile)
            query_count = 0
            query_seconds = 0.0

            def query_wrapper(execute, sql, params, many, context):
                nonlocal query_count, query_seconds
                query_start = perf_counter()
                try:
                    return execute(sql, params, many, context)
                finally:
                    query_duration = perf_counter() - query_start
                    query_count += 1
                    query_seconds += query_duration
                    group = query_groups[_classify_sql(sql)]
                    group.count += 1
                    group.seconds += query_duration

            start = perf_counter()
            rss_before_mib = _rss_mib()
            try:
                with connection.execute_wrapper(query_wrapper):
                    return original(*args, **kwargs)
            finally:
                profile = profiles.setdefault(name, StepProfile())
                if profile.calls == 0:
                    profile.rss_before_mib = rss_before_mib
                profile.calls += 1
                profile.seconds += perf_counter() - start
                profile.query_count += query_count
                profile.query_seconds += query_seconds
                profile.rss_after_mib = _rss_mib()
                profile.rss_peak_mib = max(profile.rss_peak_mib, _peak_rss_mib())
                if profile.query_groups is None:
                    profile.query_groups = {}
                for group_name, group in query_groups.items():
                    total = profile.query_groups.setdefault(
                        group_name, QueryGroupProfile()
                    )
                    total.count += group.count
                    total.seconds += group.seconds

        return patch(f"synch.tasks.{name}", profiled_step)

    def _write_report(
        self,
        *,
        seed_seconds: float,
        total_seconds: float,
        total_query_count: int,
        step_profiles: dict[str, StepProfile],
        turn_client: BenchmarkTurnClient,
        scale: BenchmarkScale,
        rss_baseline_mib: float,
        rss_after_sync_mib: float,
    ) -> None:
        self.stdout.write("")
        self.stdout.write("sync_all benchmark")
        self.stdout.write(f"  seed_seconds: {seed_seconds:.3f}")
        self.stdout.write(f"  total_seconds: {total_seconds:.3f}")
        self.stdout.write(f"  total_queries: {total_query_count}")
        self.stdout.write(f"  rss_baseline_mib: {rss_baseline_mib:.1f}")
        self.stdout.write(f"  rss_peak_mib: {_peak_rss_mib():.1f}")
        self.stdout.write(f"  rss_after_sync_mib: {rss_after_sync_mib:.1f}")
        for name, profile in sorted(
            step_profiles.items(),
            key=lambda item: item[1].seconds,
            reverse=True,
        ):
            self.stdout.write(
                f"  {name}: {profile.seconds:.3f}s over {profile.calls} call(s) "
                f"({profile.query_count} queries, "
                f"{profile.query_seconds:.3f}s DB)"
            )
            self.stdout.write(
                f"    rss_before_mib: {profile.rss_before_mib:.1f}, "
                f"rss_after_mib: {profile.rss_after_mib:.1f}, "
                f"rss_peak_mib: {profile.rss_peak_mib:.1f}"
            )
            query_groups = profile.query_groups or {}
            for group_name, group_profile in sorted(
                query_groups.items(),
                key=lambda item: item[1].seconds,
                reverse=True,
            )[:5]:
                self.stdout.write(
                    f"    {group_name}: {group_profile.count} queries, "
                    f"{group_profile.seconds:.3f}s DB"
                )
        self.stdout.write(
            "  turn_import_rows: "
            + ", ".join(str(count) for count in turn_client.import_counts)
        )
        self.stdout.write(
            "  scale: "
            f"{scale.facilities} facilities, {scale.patients} patients, "
            f"{scale.prescriptions} prescriptions, "
            f"{scale.prescription_updates} prescription updates, "
            f"{scale.patient_updates} patient updates, "
            f"{scale.new_patients} new patients, "
            f"{scale.phone_changes} phone changes"
        )

    def _time(self, func) -> float:
        start = perf_counter()
        func()
        return perf_counter() - start


def _patient_id(index: int) -> str:
    return f"{BENCHMARK_PREFIX}-patient-{index:05d}"


def _prescription_id(index: int) -> str:
    return f"{BENCHMARK_PREFIX}-prescription-{index:05d}"


def _facility_id(index: int) -> int:
    return FACILITY_ID_OFFSET + index


def _current_phone(index: int) -> str:
    return f"082{index:07d}"


def _active_phone(index: int, scale: BenchmarkScale) -> str:
    if index < scale.new_patients + scale.phone_changes:
        return f"+2781{index:07d}"
    return f"+2782{index:07d}"


def _patient_record(index: int, date_updated: datetime) -> dict[str, Any]:
    return {
        "id": _patient_id(index),
        "date_created": "2026-03-26 06:00:00.000",
        "date_updated": date_updated.strftime(synch_tasks.CCMDD_TIMESTAMP_FORMAT),
        **_patient_payload(),
    }


def _prescription_record(
    *,
    index: int,
    patient_index: int,
    facility_count: int,
    date_updated: datetime,
) -> dict[str, Any]:
    return {
        "id": _prescription_id(index),
        "date_created": "2026-06-24 06:00:00.000",
        "date_updated": date_updated.strftime(synch_tasks.CCMDD_TIMESTAMP_FORMAT),
        "facility_id": _facility_id(index % facility_count),
        "patient_id": _patient_id(patient_index),
        "patient_phone": _current_phone(patient_index),
        "department_id": 1,
        "return_dates": _return_dates(),
        **_prescription_payload(),
    }


def _rss_mib() -> float:
    with Path("/proc/self/statm").open(encoding="ascii") as statm:
        resident_pages = int(statm.read().split()[1])
    return resident_pages * os.sysconf("SC_PAGE_SIZE") / 1024**2


def _peak_rss_mib() -> float:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


def _facility_payload() -> dict[str, Any]:
    return {
        "benchmark_sync_all": True,
        "fax": None,
        "email": None,
        "active": 1,
        "deleted": 0,
        "manager": None,
        "cellphone": None,
        "ownership": "Gov Province",
        "level_id_1": 1,
        "level_id_2": 333920,
        "level_id_3": 757012,
        "level_id_4": 438613,
        "level_id_5": 7758690,
        "population": "Urban",
        "level_uid_1": "JLA7wl59oN3",
        "level_uid_2": "HpotDYGlHB5",
        "level_uid_3": "CT8lZ9vouCK",
        "level_uid_4": "G9ieiEXTovG",
        "level_uid_5": "JJP5aUj398L",
        "date_created": "2026-07-23 11:48:54.687",
        "date_updated": "2026-07-23 11:48:54.687",
        "level_code_1": "za South Africa",
        "level_code_2": "gp Gauteng Province",
        "level_code_3": "gp City of Johannesburg Metropolitan Municipality",
        "level_code_4": "gp Johannesburg F Health sub-District",
        "level_code_5": "gp Johannesburg Health District Wellness Clinic",
        "level_desc_1": "South Africa",
        "level_desc_2": "Gauteng",
        "level_desc_3": "Johannesburg MM",
        "level_desc_4": "Johannesburg F SD",
        "nhi_district": 0,
        "classification": "Clinic",
    }


def _facility_record(index: int) -> dict[str, Any]:
    return {
        "id": _facility_id(index),
        "level_desc_5": f"Benchmark Clinic {index + 1}",
        "latitude": "-26.2041",
        "longitude": "28.0473",
        "telephone": "",
        "address_1": "Benchmark Street",
        "address_2": "",
        **_facility_payload(),
    }


def _patient_payload() -> dict[str, Any]:
    return {
        "benchmark_sync_all": True,
        "gender": 3,
        "surname": "Example",
        "language": "English",
        "firstname": "Benchmark",
        "date_of_birth": "1992-09-19",
        "hprs_reference": "1234567898",
        "gender_description": "Female",
        "hprs_last_update_date": None,
    }


def _prescription_payload() -> dict[str, Any]:
    return {
        "benchmark_sync_all": True,
        "repeats": None,
        "nok_contact": None,
        "nok_surname": None,
        "pharmacy_id": None,
        "serviceable": 0,
        "nok_relation": None,
        "patient_ward": None,
        "nok_firstname": None,
        "patient_address": "123 Benchmark Street\nEXAMPLE\n1234",
        "prescribed_date": "2026-10-12",
        "prescribed_time": "08:45:06.0000000",
        "prescriber_name": "Benchmark Prescriber",
        "submission_date": "2026-10-12",
        "collection_dates": [],
        "patient_phone_alt": None,
        "prescriber_number": "12345613",
        "prescribing_level": 1,
        "date_api_last_read": None,
        "patient_smartphone": 1,
        "prescriber_surname": "Example",
        "status_description": "Submitted",
        "date_api_first_read": None,
        "prescription_length": None,
        "service_provider_id": -1,
        "clinic_file_reference": "BENCH123456",
        "prescription_status_id": 5,
        "date_api_confirmed_data": None,
        "prescribing_level_description": "Primary Health Care",
    }


def _return_dates() -> list[dict[str, Any]]:
    today = django_timezone.localdate()
    return [
        {
            "note": "Testing",
            "day_count": 28,
            "description": "1 Month",
            "return_date": (today + timedelta(days=28)).isoformat(),
        },
        {
            "note": "Benchmark",
            "day_count": 168,
            "description": "6 Months",
            "return_date": (today + timedelta(days=168)).isoformat(),
        },
    ]


def _patient_context(index: int, scale: BenchmarkScale) -> dict[str, Any]:
    if index < scale.appointment_updates or index >= scale.prescriptions:
        return {}
    last_prescription_index = (
        index + ((scale.prescriptions - 1 - index) // scale.patients) * scale.patients
    )
    facility_index = last_prescription_index % scale.facilities
    return {
        "turn_appointment_context_urn": f"+27{_current_phone(index)[1:]}",
        "turn_appointment_context_patient_id": _patient_id(index),
        "turn_appointment_context_next_appointment_date": (
            django_timezone.localdate() + timedelta(days=28)
        ),
        "turn_appointment_context_facility_name": (
            f"Benchmark Clinic {facility_index + 1}"
        ),
        "turn_appointment_context_facility_latitude": Decimal("-26.2041"),
        "turn_appointment_context_facility_longitude": Decimal("28.0473"),
    }


def _classify_sql(sql: str) -> str:
    sql_upper = sql.upper()

    if sql_upper.startswith("SELECT"):
        if '"synch_prescription"' in sql:
            if '"patient_id"' in sql and "WHERE" in sql_upper:
                return "select prescriptions for patient"
            if "MAX(" in sql_upper and '"date_updated"' in sql:
                return "select prescription watermark"
            return "select prescriptions"
        if '"synch_facility"' in sql:
            if '"ccmdd_facility_id" IN' in sql:
                return "select facilities by CCMDD id"
            return "select facilities"
        if '"synch_patient"' in sql:
            if "MAX(" in sql_upper and '"date_updated"' in sql:
                return "select patient watermark"
            return "select patients"
        if '"lock_lock"' in sql:
            return "select lock"
        return "select other"

    if sql_upper.startswith("INSERT"):
        if '"synch_facility"' in sql:
            return "insert/upsert facilities"
        if '"synch_patient"' in sql:
            return "insert/upsert patients"
        if '"synch_prescription"' in sql:
            return "insert/upsert prescriptions"
        if '"lock_lock"' in sql:
            return "insert lock"
        return "insert other"

    if sql_upper.startswith("UPDATE"):
        if '"synch_patient"' in sql:
            return "update patients"
        if '"lock_lock"' in sql:
            return "update lock"
        return "update other"

    if sql_upper.startswith("DELETE"):
        if '"lock_lock"' in sql:
            return "delete lock"
        return "delete other"

    if sql_upper.startswith("SAVEPOINT") or sql_upper.startswith("RELEASE SAVEPOINT"):
        return "savepoint"

    return "other"
