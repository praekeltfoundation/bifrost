from __future__ import annotations

import csv
from collections.abc import Callable, Mapping, Sequence
from io import StringIO
from random import uniform
from time import sleep
from urllib.parse import urljoin

from requests import HTTPError, RequestException, Session, Timeout

REQUEST_TIMEOUT_SECONDS = 30
OTP_REQUEST_TIMEOUT_SECONDS = 10
RETRY_LIMIT = 5
TURN_CONTACTS_CSV_MAX_BYTES = 1024 * 1024

RETRYABLE_HTTP_STATUS_CODES = {408, 409, 425, 429, 500, 502, 503, 504}


class TurnAPIError(Exception):
    pass


class TurnRetryExhausted(TurnAPIError):
    pass


class TurnRowTooLargeError(TurnAPIError):
    pass


class TurnOTPAPIError(Exception):
    pass


class TurnOTPUpstreamError(TurnOTPAPIError):
    pass


class TurnOTPTimeoutError(TurnOTPAPIError):
    pass


class TurnAPIClient:
    def __init__(self, base_url: str, token: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.sleep: Callable[[float], None] = sleep
        self.random_uniform: Callable[[float, float], float] = uniform
        self.session = Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.v1+json",
            }
        )

    def import_contacts(self, rows: list[dict[str, object]]) -> list[dict[str, str]]:
        if not rows:
            return []

        fieldnames = self._get_fieldnames(rows)
        batches = self._build_batches(rows, fieldnames)

        errors: list[dict[str, str]] = []
        for batch in batches:
            response = self._request(
                method="POST",
                url=urljoin(self.base_url, "/v1/contacts"),
                data=batch,
            )
            errors.extend(self._extract_error_rows(response.text))
        return errors

    def update_contact_profile(
        self,
        *,
        contact_id: str,
        fields: Mapping[str, str],
    ) -> None:
        self._request(
            method="PATCH",
            url=urljoin(self.base_url, f"/v1/contacts/{contact_id}/profile"),
            json=dict(fields),
        )

    def send_template_message(
        self,
        *,
        msisdn: str,
        template_namespace: str,
        template_name: str,
        template_language: str,
        body_parameters: Sequence[str],
    ) -> str:
        response = self._request(
            method="POST",
            url=urljoin(self.base_url, "/v1/messages"),
            json={
                "to": msisdn,
                "type": "template",
                "template": {
                    "namespace": template_namespace,
                    "name": template_name,
                    "language": {
                        "code": template_language,
                        "policy": "deterministic",
                    },
                    "components": [
                        {
                            "type": "body",
                            "parameters": [
                                {"type": "text", "text": value}
                                for value in body_parameters
                            ],
                        }
                    ],
                },
            },
        )

        try:
            payload = response.json()
            message_id = payload["messages"][0]["id"]
        except (KeyError, IndexError, TypeError, ValueError) as exc:
            raise TurnAPIError(
                "Turn template response did not include a provider message ID."
            ) from exc

        if not isinstance(message_id, str) or not message_id:
            raise TurnAPIError(
                "Turn template response did not include a provider message ID."
            )

        return message_id

    def _get_fieldnames(self, rows: Sequence[dict[str, object]]) -> list[str]:
        fieldnames: list[str] = []
        seen: set[str] = set()
        for row in rows:
            for key in row:
                if key not in seen:
                    seen.add(key)
                    fieldnames.append(key)

        if "urn" not in seen:
            raise TurnAPIError("Turn CSV imports require an 'urn' column.")

        return fieldnames

    def _build_batches(
        self, rows: Sequence[dict[str, object]], fieldnames: Sequence[str]
    ) -> list[bytes]:
        batches: list[bytes] = []
        header = current_batch = self._serialize_header(fieldnames)
        header_size = len(header)

        for row in rows:
            row_csv = self._serialize_row(row, fieldnames)
            row_size = len(row_csv)
            if len(current_batch) + row_size <= TURN_CONTACTS_CSV_MAX_BYTES:
                current_batch += row_csv
                continue
            if header_size + row_size > TURN_CONTACTS_CSV_MAX_BYTES:
                raise TurnRowTooLargeError(
                    "A single contact row exceeds the Turn CSV batch size limit."
                )
            batches.append(current_batch)
            current_batch = header + row_csv

        if current_batch != header:
            batches.append(current_batch)

        return batches

    def _serialize_header(self, fieldnames: Sequence[str]) -> bytes:
        buffer = StringIO(newline="")
        writer = csv.DictWriter(buffer, fieldnames=fieldnames)
        writer.writeheader()
        return buffer.getvalue().encode("utf-8")

    def _serialize_row(
        self, row: dict[str, object], fieldnames: Sequence[str]
    ) -> bytes:
        buffer = StringIO(newline="")
        writer = csv.DictWriter(buffer, fieldnames=fieldnames)
        writer.writerow(row)
        return buffer.getvalue().encode("utf-8")

    def _extract_error_rows(self, csv_text: str) -> list[dict[str, str]]:
        reader = csv.DictReader(StringIO(csv_text))
        errors: list[dict[str, str]] = []
        for row in reader:
            if any(value.startswith("ERROR:") for value in row.values()):
                errors.append(row)
        return errors

    def _request(
        self,
        method: str,
        url: str,
        data: bytes | None = None,
        json: Mapping[str, object] | None = None,
        timeout: int = REQUEST_TIMEOUT_SECONDS,
    ):
        headers = None
        if data is not None:
            headers = {"Content-Type": "text/csv"}
        elif json is not None:
            headers = {"Content-Type": "application/json"}

        retry_attempt = 0
        while True:
            try:
                if data is not None and json is not None:
                    response = self.session.request(
                        method=method,
                        url=url,
                        data=data,
                        json=json,
                        headers=headers,
                        timeout=timeout,
                    )
                elif data is not None:
                    response = self.session.request(
                        method=method,
                        url=url,
                        data=data,
                        headers=headers,
                        timeout=timeout,
                    )
                elif json is not None:
                    response = self.session.request(
                        method=method,
                        url=url,
                        json=json,
                        headers=headers,
                        timeout=timeout,
                    )
                else:
                    response = self.session.request(
                        method=method,
                        url=url,
                        timeout=timeout,
                    )
            except RequestException as exc:
                if retry_attempt >= RETRY_LIMIT:
                    raise TurnRetryExhausted(
                        f"Temporary Turn request failure for {method} {url}"
                    ) from exc
                self._sleep_before_retry(retry_attempt, retry_after=None)
                retry_attempt += 1
                continue

            if response.status_code in RETRYABLE_HTTP_STATUS_CODES:
                if retry_attempt >= RETRY_LIMIT:
                    raise TurnRetryExhausted(
                        "Temporary Turn response "
                        f"{response.status_code} for {method} {url}",
                    )
                retry_after = self._parse_retry_after(
                    response.headers.get("Retry-After")
                )
                self._sleep_before_retry(retry_attempt, retry_after=retry_after)
                retry_attempt += 1
                continue

            try:
                response.raise_for_status()
            except HTTPError as exc:
                raise TurnAPIError(
                    "Turn request failed with status "
                    f"{response.status_code} for {method} {url}",
                ) from exc

            return response

    def _sleep_before_retry(
        self,
        retry_attempt: int,
        retry_after: float | None,
    ) -> None:
        max_delay_seconds = float(2**retry_attempt)
        jitter_delay_seconds = self.random_uniform(0, max_delay_seconds)
        delay_seconds = max(jitter_delay_seconds, retry_after or 0.0)
        self.sleep(delay_seconds)

    def _parse_retry_after(self, retry_after: str | None) -> float | None:
        if retry_after is None:
            return None
        try:
            return max(float(retry_after), 0.0)
        except ValueError:
            return None


class TurnOTPAPIClient:
    def __init__(
        self,
        *,
        base_url: str,
        token: str,
        template_namespace: str,
        template_name: str,
        template_language: str,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.template_namespace = template_namespace
        self.template_name = template_name
        self.template_language = template_language
        self.session = Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.v1+json",
                "Content-Type": "application/json",
            }
        )

    def send_authentication_template_message(self, *, msisdn: str, otp: str) -> str:
        try:
            response = self.session.request(
                method="POST",
                url=urljoin(self.base_url, "/v1/messages"),
                json={
                    "to": msisdn,
                    "type": "template",
                    "template": {
                        "namespace": self.template_namespace,
                        "name": self.template_name,
                        "language": {
                            "code": self.template_language,
                            "policy": "deterministic",
                        },
                        "components": [
                            {
                                "type": "body",
                                "parameters": [{"type": "text", "text": otp}],
                            },
                            {
                                "type": "button",
                                "sub_type": "url",
                                "index": "0",
                                "parameters": [{"type": "text", "text": otp}],
                            },
                        ],
                    },
                },
                timeout=OTP_REQUEST_TIMEOUT_SECONDS,
            )
        except RequestException as exc:
            if isinstance(exc, HTTPError):
                raise TurnOTPUpstreamError("Turn OTP request failed.") from exc
            if isinstance(exc, Timeout):
                raise TurnOTPTimeoutError("Turn OTP request timed out.") from exc
            raise TurnOTPUpstreamError("Turn OTP request failed.") from exc

        try:
            response.raise_for_status()
        except HTTPError as exc:
            raise TurnOTPUpstreamError(
                f"Turn OTP request failed with status {response.status_code}."
            ) from exc

        try:
            payload = response.json()
            message_id = payload["messages"][0]["id"]
        except (KeyError, IndexError, TypeError, ValueError) as exc:
            raise TurnOTPUpstreamError(
                "Turn OTP response did not include a provider message ID."
            ) from exc

        if not isinstance(message_id, str) or not message_id:
            raise TurnOTPUpstreamError(
                "Turn OTP response did not include a provider message ID."
            )

        return message_id
