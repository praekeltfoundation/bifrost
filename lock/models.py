from __future__ import annotations

from datetime import datetime, timedelta
from uuid import uuid4

from django.db import IntegrityError, models, transaction
from django.utils import timezone


class LockAcquisitionError(Exception):
    pass


class LockOwnershipError(Exception):
    pass


class Lock(models.Model):
    DEFAULT_TTL = timedelta(hours=1)

    key: models.CharField[str, str] = models.CharField(max_length=255, unique=True)
    owner: models.CharField[str, str] = models.CharField(max_length=255)
    ttl: models.DurationField[timedelta, timedelta] = models.DurationField()
    expires_at: models.DateTimeField[datetime, datetime] = models.DateTimeField()
    created_at: models.DateTimeField[datetime, datetime] = models.DateTimeField(
        auto_now_add=True
    )
    updated_at: models.DateTimeField[datetime, datetime] = models.DateTimeField(
        auto_now=True
    )

    def __str__(self):
        return f"{self.key}:{self.owner}"

    @classmethod
    def acquire(cls, key: str, owner: str | None = None, ttl: timedelta | None = None):
        owner = owner or uuid4().hex
        now = timezone.now()

        with transaction.atomic():
            try:
                ttl_to_use = ttl or cls.DEFAULT_TTL
                with transaction.atomic():
                    return cls.objects.create(
                        key=key,
                        owner=owner,
                        expires_at=now + ttl_to_use,
                        ttl=ttl_to_use,
                    )
            except IntegrityError:
                lock = cls.objects.select_for_update().get(key=key)
                if lock.expires_at > now and lock.owner != owner:
                    raise LockAcquisitionError(
                        f"Lock '{key}' is already held by '{lock.owner}'."
                    ) from None

                ttl_to_use = ttl or lock.ttl
                lock.owner = owner
                lock.expires_at = now + ttl_to_use
                lock.ttl = ttl_to_use
                lock.save(update_fields=["owner", "expires_at", "updated_at", "ttl"])
                return lock

    def refresh(self):
        now = timezone.now()

        with transaction.atomic():
            lock = type(self).objects.select_for_update().get(pk=self.pk)
            if lock.owner != self.owner:
                raise LockOwnershipError(
                    f"Lock '{lock.key}' is owned by '{lock.owner}', not '{self.owner}'."
                )

            lock.expires_at = now + lock.ttl
            lock.save(update_fields=["expires_at", "updated_at"])
            return lock

    def release(self):
        with transaction.atomic():
            lock = type(self).objects.select_for_update().get(pk=self.pk)
            if lock.owner != self.owner:
                raise LockOwnershipError(
                    f"Lock '{lock.key}' is owned by '{lock.owner}', not '{self.owner}'."
                )

            lock.delete()
