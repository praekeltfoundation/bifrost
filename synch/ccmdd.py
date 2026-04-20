from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime
from enum import IntEnum
from random import uniform
from time import sleep
from typing import Any
from urllib.parse import urljoin

from requests import HTTPError, RequestException, Session
from requests.auth import HTTPDigestAuth

LONG_RUNNING_POLL_INTERVAL_SECONDS = 5 * 60
LONG_RUNNING_STATUS_POLL_LIMIT = 12
REQUEST_TIMEOUT_SECONDS = 30
RETRY_LIMIT = 5

RETRYABLE_HTTP_STATUS_CODES = {408, 409, 425, 429, 500, 502, 503, 504}


class CCMDDOperationResult(IntEnum):
    IMMEDIATE = 1
    LONG_RUNNING_OPERATION = 2
    MULTI_LONG_RUNNING_OPERATION = 3


class CCMDDAPIError(Exception):
    pass


class CCMDDRetryExhausted(CCMDDAPIError):
    pass


class CCMDDLongRunningOperationTimeout(CCMDDAPIError):
    pass


class CCMDDAPIClient:
    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.sleep = sleep
        self.random_uniform = uniform
        self.session = Session()
        self.session.auth = HTTPDigestAuth(username, password)

    def iter_limited_prescriptions(
        self,
        date_updated: datetime | None = None,
    ) -> Iterator[dict[str, Any]]:
        filters = {}
        if date_updated is not None:
            filters["date_updated"] = date_updated.strftime("%Y-%m-%d %H:%M:%S.%f")
        yield from self._iter_limited_records(
            endpoint_path="/wapi/prescriptionLimited", filters=filters
        )

    def iter_limited_patients(
        self,
        date_updated: datetime | None = None,
    ) -> Iterator[dict[str, Any]]:
        filters = {}
        if date_updated is not None:
            filters["date_updated"] = date_updated.strftime("%Y-%m-%d %H:%M:%S.%f")
        yield from self._iter_limited_records(
            endpoint_path="/wapi/patientLimited",
            filters=filters,
        )

    def _iter_limited_records(
        self,
        endpoint_path: str,
        filters: dict,
    ) -> Iterator[dict[str, Any]]:
        response = self._request(
            method="POST",
            url=urljoin(self.base_url, endpoint_path),
            json=filters,
        )
        payload = response.json()

        if response.status_code == 202:
            yield from self._iter_long_running_records(payload)
            return

        yield from payload["data"]

    def _iter_long_running_records(
        self, payload: dict[str, Any]
    ) -> Iterator[dict[str, Any]]:
        operations = self._extract_operations(payload)
        for operation in operations:
            self._wait_for_operation(operation["status_location"])
            resource_response = self._request(
                method="GET",
                url=operation["resource_location"],
                json=None,
            )
            payload = resource_response.json()
            yield from payload["data"]

    def _wait_for_operation(self, status_url: str) -> None:
        for _ in range(LONG_RUNNING_STATUS_POLL_LIMIT):
            self.sleep(LONG_RUNNING_POLL_INTERVAL_SECONDS)
            response = self._request(method="GET", url=status_url, json=None)
            payload = response.json()
            if payload["data"]["status"] == "succeeded":
                return
        raise CCMDDLongRunningOperationTimeout(
            "Timed out waiting for CCMDD long-running operation to succeed.",
        )

    def _request(
        self,
        method: str,
        url: str,
        json: dict[str, Any] | None,
    ):
        retry_attempt = 0
        while True:
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    json=json,
                    timeout=REQUEST_TIMEOUT_SECONDS,
                )
            except RequestException as exc:
                if retry_attempt >= RETRY_LIMIT:
                    raise CCMDDRetryExhausted(
                        f"Temporary CCMDD request failure for {method} {url}",
                    ) from exc
                self._sleep_before_retry(retry_attempt)
                retry_attempt += 1
                continue

            if response.status_code in RETRYABLE_HTTP_STATUS_CODES:
                if retry_attempt >= RETRY_LIMIT:
                    raise CCMDDRetryExhausted(
                        "Temporary CCMDD response "
                        f"{response.status_code} for {method} {url}",
                    )
                self._sleep_before_retry(retry_attempt)
                retry_attempt += 1
                continue

            try:
                response.raise_for_status()
            except HTTPError as exc:
                raise CCMDDAPIError(
                    "CCMDD request failed with status "
                    f"{response.status_code} for {method} {url}",
                ) from exc

            return response

    def _sleep_before_retry(self, retry_attempt: int) -> None:
        max_delay_seconds = float(2**retry_attempt)
        delay_seconds = self.random_uniform(0, max_delay_seconds)
        self.sleep(delay_seconds)

    def _extract_operations(self, payload: dict[str, Any]) -> list[dict[str, str]]:
        result = CCMDDOperationResult(payload["result"])
        if result is CCMDDOperationResult.LONG_RUNNING_OPERATION:
            return [payload["response"]]
        if result is CCMDDOperationResult.MULTI_LONG_RUNNING_OPERATION:
            return payload["responses"]
        raise CCMDDAPIError("Expected CCMDD long-running operation response.")
