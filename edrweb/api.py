from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from time import sleep
from typing import Any
from urllib.parse import urljoin

from requests import HTTPError, RequestException, Response, Session

REQUEST_TIMEOUT_SECONDS = 60
PAGE_SIZE = 500
RETRY_LIMIT = 5
RETRY_DELAYS_SECONDS = (30.0, 60.0, 120.0, 240.0)
RETRYABLE_HTTP_STATUS_CODES = {429, 500, 503}
TOKEN_EXPIRY_BUFFER = timedelta(seconds=60)


class EDRWebAPIError(Exception):
    pass


class EDRWebRetryExhausted(EDRWebAPIError):
    pass


class EDRWebUnauthorized(EDRWebAPIError):
    pass


@dataclass
class EDRWebToken:
    access_token: str
    refresh_token: str
    expires_at: datetime
    refresh_expires_at: datetime


class EDRWebAPIClient:
    def __init__(
        self,
        *,
        base_url: str,
        username: str,
        password: str,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.sleep = sleep
        self.now = lambda: datetime.now(tz=timezone.utc)
        self.session = Session()
        self._token: EDRWebToken | None = None

    def iter_appointment_reminder_records(
        self,
        updated_since: datetime | None = None,
    ) -> Iterator[dict[str, Any]]:
        if updated_since is not None and updated_since.utcoffset() is None:
            raise ValueError("updated_since must include a timezone offset.")

        self._ensure_access_token()
        cursor: str | None = None

        while True:
            params: dict[str, object] = {
                "pageSize": PAGE_SIZE,
                "upcomingOnly": "true",
            }
            if updated_since is not None:
                params["updatedSince"] = updated_since.isoformat()
            if cursor is not None:
                params["cursor"] = cursor

            payload = self._request_protected(
                method="GET",
                path="/persons/appointment-reminders",
                params=params,
            )
            persons = payload.get("Persons", [])
            if not isinstance(persons, list):
                raise EDRWebAPIError("EDRWeb Persons field must be a list.")
            yield from persons

            has_more = payload.get("HasMore")
            if not isinstance(has_more, bool):
                raise EDRWebAPIError("EDRWeb HasMore field must be a boolean.")
            if not has_more:
                return

            cursor = payload.get("NextCursor")
            if not isinstance(cursor, str) or not cursor:
                raise EDRWebAPIError(
                    "EDRWeb NextCursor is required when HasMore is true."
                )

    def _ensure_access_token(self) -> None:
        if self._token is None:
            self._token = self._authenticate()
        if self._token.expires_at <= self.now() + TOKEN_EXPIRY_BUFFER:
            if self._token.refresh_expires_at <= self.now() + TOKEN_EXPIRY_BUFFER:
                self._token = self._authenticate()
            else:
                self._token = self._refresh_token()

    def _authenticate(self) -> EDRWebToken:
        payload = self._request_json(
            method="POST",
            path="/auth/token",
            json={"username": self.username, "password": self.password},
        )
        return self._parse_token_response(payload)

    def _request_protected(
        self,
        *,
        method: str,
        path: str,
        params: dict[str, object],
    ) -> dict[str, Any]:
        if self._token is None:
            raise EDRWebAPIError("EDRWeb access token is missing.")
        try:
            return self._request_json(
                method=method,
                path=path,
                headers={"Authorization": f"Bearer {self._token.access_token}"},
                params=params,
            )
        except EDRWebUnauthorized:
            self._token = self._refresh_token()
            return self._request_json(
                method=method,
                path=path,
                headers={"Authorization": f"Bearer {self._token.access_token}"},
                params=params,
            )

    def _refresh_token(self) -> EDRWebToken:
        if self._token is None:
            return self._authenticate()
        try:
            payload = self._request_json(
                method="POST",
                path="/auth/token/refresh",
                json={"RefreshToken": self._token.refresh_token},
            )
        except EDRWebUnauthorized:
            return self._authenticate()
        return self._parse_token_response(payload)

    def _request_json(
        self,
        *,
        method: str,
        path: str,
        json: dict[str, object] | None = None,
        headers: dict[str, str] | None = None,
        params: dict[str, object] | None = None,
    ) -> dict[str, Any]:
        url = self._url(path)
        for retry_attempt in range(RETRY_LIMIT):
            try:
                request_kwargs: dict[str, Any] = {
                    "method": method,
                    "url": url,
                    "timeout": REQUEST_TIMEOUT_SECONDS,
                }
                if headers is not None:
                    request_kwargs["headers"] = headers
                if params is not None:
                    request_kwargs["params"] = params
                if json is not None:
                    request_kwargs["json"] = json
                response = self.session.request(
                    **request_kwargs,
                )
            except RequestException as exc:
                self._retry_or_raise(
                    retry_attempt=retry_attempt,
                    method=method,
                    url=url,
                    retry_after=None,
                    cause=exc,
                )
                continue

            if response.status_code in RETRYABLE_HTTP_STATUS_CODES:
                self._retry_or_raise(
                    retry_attempt=retry_attempt,
                    method=method,
                    url=url,
                    retry_after=self._parse_retry_after(
                        response.headers.get("Retry-After")
                    ),
                    cause=None,
                )
                continue

            try:
                response.raise_for_status()
            except HTTPError as exc:
                if response.status_code == 401:
                    raise EDRWebUnauthorized(
                        f"EDRWeb request was unauthorized for {method} {url}",
                    ) from exc
                raise EDRWebAPIError(
                    "EDRWeb request failed with status "
                    f"{response.status_code} for {method} {url}",
                ) from exc

            try:
                payload = response.json()
            except ValueError as exc:
                self._retry_or_raise(
                    retry_attempt=retry_attempt,
                    method=method,
                    url=url,
                    retry_after=None,
                    cause=exc,
                    response=response,
                )
                continue

            if not isinstance(payload, dict):
                raise EDRWebAPIError("EDRWeb response must be a JSON object.")
            return payload

        raise EDRWebRetryExhausted(
            f"Temporary EDRWeb failure for {method} {url}",
        )

    def _retry_or_raise(
        self,
        *,
        retry_attempt: int,
        method: str,
        url: str,
        retry_after: float | None,
        cause: Exception | None,
        response: Response | None = None,
    ) -> None:
        if retry_attempt >= RETRY_LIMIT - 1:
            message = f"Temporary EDRWeb failure for {method} {url}"
            if response is not None:
                message = f"{message}: {response.text}"
            raise EDRWebRetryExhausted(message) from cause

        delay_seconds = max(RETRY_DELAYS_SECONDS[retry_attempt], retry_after or 0.0)
        self.sleep(delay_seconds)

    def _parse_retry_after(self, retry_after: str | None) -> float | None:
        if retry_after is None:
            return None
        try:
            return max(float(retry_after), 0.0)
        except ValueError:
            return None

    def _parse_token_response(self, payload: dict[str, Any]) -> EDRWebToken:
        try:
            access_token = payload["AccessToken"]
            refresh_token = payload["RefreshToken"]
            expires_at = payload["ExpiresAt"]
            refresh_expires_at = payload["RefreshTokenExpiresAt"]
        except KeyError as exc:
            raise EDRWebAPIError(
                f"EDRWeb token response missing field {exc.args[0]}."
            ) from exc

        return EDRWebToken(
            access_token=access_token,
            refresh_token=refresh_token,
            expires_at=datetime.fromisoformat(expires_at),
            refresh_expires_at=datetime.fromisoformat(refresh_expires_at),
        )

    def _url(self, path: str) -> str:
        return urljoin(f"{self.base_url}/", path.lstrip("/"))
