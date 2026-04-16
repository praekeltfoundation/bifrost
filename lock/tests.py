from datetime import timedelta
from unittest.mock import patch

from django.test import TestCase
from django.utils import timezone

from lock.models import Lock, LockAcquisitionError, LockOwnershipError


class LockAcquireTests(TestCase):
    def test_acquire_creates_lock_for_explicit_owner(self):
        lock = Lock.acquire(key="daily-sync", owner="worker-1")

        self.assertEqual(lock.key, "daily-sync")
        self.assertEqual(lock.owner, "worker-1")
        self.assertGreater(lock.expires_at, timezone.now())

    def test_acquire_generates_owner_when_not_provided(self):
        lock = Lock.acquire(key="daily-sync")

        self.assertIsInstance(lock.owner, str)

    def test_acquire_raises_for_different_owner_while_lock_is_active(self):
        Lock.acquire(key="daily-sync", owner="worker-1")

        with self.assertRaises(LockAcquisitionError):
            Lock.acquire(key="daily-sync", owner="worker-2")

    def test_acquire_raises_for_different_generated_owner_while_lock_is_active(self):
        Lock.acquire(key="daily-sync")

        with self.assertRaises(LockAcquisitionError):
            Lock.acquire(key="daily-sync")

    def test_acquire_reuses_existing_row_after_expiry(self):
        initial_time = timezone.now()
        later_time = initial_time + Lock.DEFAULT_TTL + timedelta(seconds=1)

        with patch("lock.models.timezone.now", return_value=initial_time):
            original_lock = Lock.acquire(key="daily-sync", owner="worker-1")

        with patch("lock.models.timezone.now", return_value=later_time):
            reacquired_lock = Lock.acquire(key="daily-sync", owner="worker-2")

        self.assertEqual(Lock.objects.count(), 1)
        self.assertEqual(reacquired_lock.pk, original_lock.pk)
        self.assertEqual(reacquired_lock.owner, "worker-2")


class LockReleaseTests(TestCase):
    def test_release_allows_new_acquisition(self):
        lock = Lock.acquire(key="daily-sync", owner="worker-1")

        lock.release()

        reacquired_lock = Lock.acquire(key="daily-sync", owner="worker-2")
        self.assertEqual(reacquired_lock.owner, "worker-2")

    def test_release_raises_for_non_owner(self):
        initial_time = timezone.now()
        later_time = initial_time + Lock.DEFAULT_TTL + timedelta(seconds=1)

        with patch("lock.models.timezone.now", return_value=initial_time):
            original_lock = Lock.acquire(key="daily-sync", owner="worker-1")

        with patch("lock.models.timezone.now", return_value=later_time):
            Lock.acquire(key="daily-sync", owner="worker-2")

        with self.assertRaises(LockOwnershipError):
            original_lock.release()


class LockRefreshTests(TestCase):
    def test_refresh_extends_expiry_for_active_owner(self):
        initial_time = timezone.now()
        refresh_time = initial_time + timedelta(minutes=5)

        with patch("lock.models.timezone.now", return_value=initial_time):
            lock = Lock.acquire(key="daily-sync", owner="worker-1")

        original_expiry = lock.expires_at

        with patch("lock.models.timezone.now", return_value=refresh_time):
            refreshed_lock = lock.refresh()

        self.assertEqual(refreshed_lock.pk, lock.pk)
        self.assertEqual(refreshed_lock.owner, "worker-1")
        self.assertGreater(refreshed_lock.expires_at, original_expiry)

    def test_refresh_raises_for_different_owner(self):
        initial_time = timezone.now()
        expired_time = initial_time + Lock.DEFAULT_TTL + timedelta(seconds=1)

        with patch("lock.models.timezone.now", return_value=initial_time):
            lock = Lock.acquire(key="daily-sync")

        with patch("lock.models.timezone.now", return_value=expired_time):
            Lock.acquire(key="daily-sync")

        with self.assertRaises(LockOwnershipError):
            lock.refresh()

    def test_refresh_reacquires_expired_lock_for_same_owner(self):
        initial_time = timezone.now()
        expired_time = initial_time + Lock.DEFAULT_TTL + timedelta(seconds=1)

        with patch("lock.models.timezone.now", return_value=initial_time):
            lock = Lock.acquire(key="daily-sync")

        with patch("lock.models.timezone.now", return_value=expired_time):
            refreshed_lock = lock.refresh()

        self.assertEqual(refreshed_lock.pk, lock.pk)
        self.assertGreater(refreshed_lock.expires_at, expired_time)

    def test_refresh_raises_if_lock_has_expired_and_owner_changed(self):
        initial_time = timezone.now()
        expired_time = initial_time + Lock.DEFAULT_TTL + timedelta(seconds=1)

        with patch("lock.models.timezone.now", return_value=initial_time):
            lock = Lock.acquire(key="daily-sync", owner="worker-1")

        with patch("lock.models.timezone.now", return_value=expired_time):
            Lock.acquire(key="daily-sync", owner="worker-2")

        with self.assertRaises(LockOwnershipError):
            lock.refresh()
