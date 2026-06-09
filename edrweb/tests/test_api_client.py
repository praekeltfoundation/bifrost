from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import Mock, call

import requests
from django.test import SimpleTestCase

from edrweb.api import EDRWebAPIClient, EDRWebAPIError, EDRWebRetryExhausted

TEST_PASSWORD = "test-password"  # noqa: S105


class EDRWebAPIClientTests(SimpleTestCase):
    def make_client(self, session: Mock | None = None) -> EDRWebAPIClient:
        client = EDRWebAPIClient(
            base_url="https://staging.edrweb.net/api",
            username="api-user",
            password=TEST_PASSWORD,
        )
        if session is not None:
            client.session = session
        return client

    def make_client_with_base_url(
        self,
        *,
        base_url: str,
        session: Mock | None = None,
    ) -> EDRWebAPIClient:
        client = EDRWebAPIClient(
            base_url=base_url,
            username="api-user",
            password=TEST_PASSWORD,
        )
        if session is not None:
            client.session = session
        return client

    def make_response(
        self,
        *,
        status_code: int = 200,
        payload: dict | None = None,
        headers: dict[str, str] | None = None,
        text: str = "",
        json_side_effect: Exception | None = None,
    ) -> Mock:
        response = Mock()
        response.status_code = status_code
        response.json.return_value = payload if payload is not None else {}
        if json_side_effect is not None:
            response.json.side_effect = json_side_effect
        response.headers = headers or {}
        response.text = text
        response.raise_for_status.side_effect = None
        if status_code >= 400:
            response.raise_for_status.side_effect = requests.HTTPError(
                f"HTTP {status_code}",
                response=response,
            )
        return response

    def test_iter_appointment_reminder_records_authenticates_and_flattens_pages(self):
        session = Mock()
        session.request.side_effect = [
            self.make_response(
                payload={
                    "AccessToken": "access-token",
                    "Type": "Bearer",
                    "ExpiresAt": "9999-06-02T08:00:00.000+00:00",
                    "RefreshToken": "refresh-token",
                    "RefreshExpiresAt": "9999-06-09T06:00:00.000+00:00",
                }
            ),
            self.make_response(
                payload={
                    "Persons": [{"PersonId": "person-1"}],
                    "NextCursor": "cursor-1",
                    "HasMore": True,
                }
            ),
            self.make_response(
                payload={"Persons": [{"PersonId": "person-2"}], "HasMore": False}
            ),
        ]
        client = self.make_client(session=session)

        records = list(client.iter_appointment_reminder_records())

        self.assertEqual(
            records,
            [{"PersonId": "person-1"}, {"PersonId": "person-2"}],
        )
        self.assertEqual(
            session.request.call_args_list,
            [
                call(
                    method="POST",
                    url="https://staging.edrweb.net/api/auth/token",
                    json={"username": "api-user", "password": TEST_PASSWORD},
                    timeout=60,
                ),
                call(
                    method="GET",
                    url="https://staging.edrweb.net/api/persons/appointment-reminders",
                    headers={"Authorization": "Bearer access-token"},
                    params={"pageSize": 500, "upcomingOnly": "true"},
                    timeout=60,
                ),
                call(
                    method="GET",
                    url="https://staging.edrweb.net/api/persons/appointment-reminders",
                    headers={"Authorization": "Bearer access-token"},
                    params={
                        "cursor": "cursor-1",
                        "pageSize": 500,
                        "upcomingOnly": "true",
                    },
                    timeout=60,
                ),
            ],
        )

    def test_iter_appointment_reminder_records_joins_base_url_with_trailing_slash(self):
        session = Mock()
        session.request.side_effect = [
            self.make_response(
                payload={
                    "AccessToken": "access-token",
                    "Type": "Bearer",
                    "ExpiresAt": "9999-06-02T08:00:00.000+00:00",
                    "RefreshToken": "refresh-token",
                    "RefreshExpiresAt": "9999-06-09T06:00:00.000+00:00",
                }
            ),
            self.make_response(payload={"HasMore": False}),
        ]
        client = self.make_client_with_base_url(
            base_url="https://staging.edrweb.net/api/",
            session=session,
        )

        records = list(client.iter_appointment_reminder_records())

        self.assertEqual(records, [])
        self.assertEqual(
            session.request.call_args_list[0].kwargs["url"],
            "https://staging.edrweb.net/api/auth/token",
        )
        self.assertEqual(
            session.request.call_args_list[1].kwargs["url"],
            "https://staging.edrweb.net/api/persons/appointment-reminders",
        )

    def test_iter_appointment_reminder_records_sends_timezone_aware_updated_since(self):
        session = Mock()
        session.request.side_effect = [
            self.make_response(
                payload={
                    "AccessToken": "access-token",
                    "Type": "Bearer",
                    "ExpiresAt": "9999-06-02T08:00:00.000+00:00",
                    "RefreshToken": "refresh-token",
                    "RefreshExpiresAt": "9999-06-09T06:00:00.000+00:00",
                }
            ),
            self.make_response(
                payload={"Persons": [], "NextCursor": "cursor-1", "HasMore": True}
            ),
            self.make_response(payload={"HasMore": False}),
        ]
        client = self.make_client(session=session)
        updated_since = datetime(2026, 6, 1, 8, 30, tzinfo=timezone.utc)

        records = list(client.iter_appointment_reminder_records(updated_since))

        self.assertEqual(records, [])
        self.assertEqual(
            session.request.call_args_list[1].kwargs["params"]["updatedSince"],
            "2026-06-01T08:30:00+00:00",
        )
        self.assertEqual(
            session.request.call_args_list[2].kwargs["params"]["updatedSince"],
            "2026-06-01T08:30:00+00:00",
        )

    def test_iter_appointment_reminder_records_rejects_naive_updated_since(self):
        session = Mock()
        client = self.make_client(session=session)

        with self.assertRaisesMessage(
            ValueError,
            "updated_since must include a timezone offset.",
        ):
            list(
                client.iter_appointment_reminder_records(
                    datetime(2026, 6, 1, 8, 30),
                )
            )
        session.request.assert_not_called()

    def test_iter_appointment_reminder_records_retries_transient_http_failures(self):
        session = Mock()
        session.request.side_effect = [
            self.make_response(
                payload={
                    "AccessToken": "access-token",
                    "Type": "Bearer",
                    "ExpiresAt": "9999-06-02T08:00:00.000+00:00",
                    "RefreshToken": "refresh-token",
                    "RefreshExpiresAt": "9999-06-09T06:00:00.000+00:00",
                }
            ),
            self.make_response(status_code=503, headers={"Retry-After": "45"}),
            self.make_response(
                payload={
                    "Persons": [{"PersonId": "person-1"}],
                    "HasMore": False,
                }
            ),
        ]
        sleep = Mock()
        client = self.make_client(session=session)
        client.sleep = sleep

        records = list(client.iter_appointment_reminder_records())

        self.assertEqual(records, [{"PersonId": "person-1"}])
        sleep.assert_called_once_with(45.0)

    def test_iter_appointment_reminder_records_refreshes_token_after_unauthorized(self):
        session = Mock()
        session.request.side_effect = [
            self.make_response(
                payload={
                    "AccessToken": "old-access-token",
                    "Type": "Bearer",
                    "ExpiresAt": "9999-06-02T08:00:00.000+00:00",
                    "RefreshToken": "refresh-token",
                    "RefreshExpiresAt": "9999-06-09T06:00:00.000+00:00",
                }
            ),
            self.make_response(status_code=401),
            self.make_response(
                payload={
                    "AccessToken": "new-access-token",
                    "Type": "Bearer",
                    "ExpiresAt": "9999-06-02T09:00:00.000+00:00",
                    "RefreshToken": "new-refresh-token",
                    "RefreshExpiresAt": "9999-06-09T07:00:00.000+00:00",
                }
            ),
            self.make_response(
                payload={
                    "Persons": [{"PersonId": "person-1"}],
                    "HasMore": False,
                }
            ),
        ]
        client = self.make_client(session=session)

        records = list(client.iter_appointment_reminder_records())

        self.assertEqual(records, [{"PersonId": "person-1"}])
        self.assertEqual(
            session.request.call_args_list[2],
            call(
                method="POST",
                url="https://staging.edrweb.net/api/auth/token/refresh",
                json={"RefreshToken": "refresh-token"},
                timeout=60,
            ),
        )
        self.assertEqual(
            session.request.call_args_list[3].kwargs["headers"],
            {"Authorization": "Bearer new-access-token"},
        )

    def test_iter_appointment_reminder_records_refreshes_expiring_access_token(self):
        session = Mock()
        session.request.side_effect = [
            self.make_response(
                payload={
                    "AccessToken": "expired-access-token",
                    "Type": "Bearer",
                    "ExpiresAt": "2026-06-01T08:00:00.000+00:00",
                    "RefreshToken": "refresh-token",
                    "RefreshExpiresAt": "9999-06-09T06:00:00.000+00:00",
                }
            ),
            self.make_response(
                payload={
                    "AccessToken": "new-access-token",
                    "Type": "Bearer",
                    "ExpiresAt": "9999-06-02T09:00:00.000+00:00",
                    "RefreshToken": "new-refresh-token",
                    "RefreshExpiresAt": "9999-06-09T07:00:00.000+00:00",
                }
            ),
            self.make_response(
                payload={"Persons": [{"PersonId": "person-1"}], "HasMore": False}
            ),
        ]
        client = self.make_client(session=session)

        records = list(client.iter_appointment_reminder_records())

        self.assertEqual(records, [{"PersonId": "person-1"}])
        self.assertEqual(
            session.request.call_args_list[1],
            call(
                method="POST",
                url="https://staging.edrweb.net/api/auth/token/refresh",
                json={"RefreshToken": "refresh-token"},
                timeout=60,
            ),
        )
        self.assertEqual(
            session.request.call_args_list[2].kwargs["headers"],
            {"Authorization": "Bearer new-access-token"},
        )

    def test_iter_appointment_reminder_records_reauthenticates_when_refresh_rejected(
        self,
    ):
        session = Mock()
        session.request.side_effect = [
            self.make_response(
                payload={
                    "AccessToken": "expired-access-token",
                    "Type": "Bearer",
                    "ExpiresAt": "2026-06-01T08:00:00.000+00:00",
                    "RefreshToken": "refresh-token",
                    "RefreshExpiresAt": "9999-06-09T06:00:00.000+00:00",
                }
            ),
            self.make_response(status_code=401),
            self.make_response(
                payload={
                    "AccessToken": "new-access-token",
                    "Type": "Bearer",
                    "ExpiresAt": "9999-06-02T09:00:00.000+00:00",
                    "RefreshToken": "new-refresh-token",
                    "RefreshExpiresAt": "9999-06-09T07:00:00.000+00:00",
                }
            ),
            self.make_response(
                payload={
                    "Persons": [{"PersonId": "person-1"}],
                    "HasMore": False,
                }
            ),
        ]
        client = self.make_client(session=session)

        records = list(client.iter_appointment_reminder_records())

        self.assertEqual(records, [{"PersonId": "person-1"}])
        self.assertEqual(
            session.request.call_args_list[2],
            call(
                method="POST",
                url="https://staging.edrweb.net/api/auth/token",
                json={"username": "api-user", "password": TEST_PASSWORD},
                timeout=60,
            ),
        )
        self.assertEqual(
            session.request.call_args_list[3].kwargs["headers"],
            {"Authorization": "Bearer new-access-token"},
        )

    def test_iter_appointment_reminder_records_reauthenticates_for_expiring_refresh(
        self,
    ):
        session = Mock()
        session.request.side_effect = [
            self.make_response(
                payload={
                    "AccessToken": "expired-access-token",
                    "Type": "Bearer",
                    "ExpiresAt": "2026-06-01T08:00:30.000+00:00",
                    "RefreshToken": "refresh-token",
                    "RefreshExpiresAt": "2026-06-01T08:00:30.000+00:00",
                }
            ),
            self.make_response(
                payload={
                    "AccessToken": "new-access-token",
                    "Type": "Bearer",
                    "ExpiresAt": "9999-06-02T09:00:00.000+00:00",
                    "RefreshToken": "new-refresh-token",
                    "RefreshExpiresAt": "9999-06-09T07:00:00.000+00:00",
                }
            ),
            self.make_response(
                payload={
                    "Persons": [{"PersonId": "person-1"}],
                    "HasMore": False,
                }
            ),
        ]
        client = self.make_client(session=session)
        client.now = Mock(return_value=datetime(2026, 6, 1, 8, 0, tzinfo=timezone.utc))

        records = list(client.iter_appointment_reminder_records())

        self.assertEqual(records, [{"PersonId": "person-1"}])
        self.assertEqual(
            session.request.call_args_list[1],
            call(
                method="POST",
                url="https://staging.edrweb.net/api/auth/token",
                json={"username": "api-user", "password": TEST_PASSWORD},
                timeout=60,
            ),
        )

    def test_iter_appointment_reminder_records_rejects_token_response_missing_fields(
        self,
    ):
        session = Mock()
        session.request.return_value = self.make_response(
            payload={
                "AccessToken": "access-token",
                "Type": "Bearer",
                "ExpiresAt": "9999-06-02T08:00:00.000+00:00",
                "RefreshToken": "refresh-token",
            }
        )
        client = self.make_client(session=session)

        with self.assertRaisesMessage(
            EDRWebAPIError,
            "EDRWeb token response missing field RefreshExpiresAt.",
        ):
            list(client.iter_appointment_reminder_records())

    def test_iter_appointment_reminder_records_exhausts_transient_retries(self):
        session = Mock()
        session.request.side_effect = [
            self.make_response(
                payload={
                    "AccessToken": "access-token",
                    "Type": "Bearer",
                    "ExpiresAt": "9999-06-02T08:00:00.000+00:00",
                    "RefreshToken": "refresh-token",
                    "RefreshExpiresAt": "9999-06-09T06:00:00.000+00:00",
                }
            ),
            *[self.make_response(status_code=500) for _ in range(5)],
        ]
        sleep = Mock()
        client = self.make_client(session=session)
        client.sleep = sleep

        with self.assertRaises(EDRWebRetryExhausted):
            list(client.iter_appointment_reminder_records())

        self.assertEqual(
            sleep.call_args_list,
            [call(30.0), call(60.0), call(120.0), call(240.0)],
        )

    def test_iter_appointment_reminder_records_retries_invalid_json_response(self):
        session = Mock()
        session.request.side_effect = [
            self.make_response(
                payload={
                    "AccessToken": "access-token",
                    "Type": "Bearer",
                    "ExpiresAt": "9999-06-02T08:00:00.000+00:00",
                    "RefreshToken": "refresh-token",
                    "RefreshExpiresAt": "9999-06-09T06:00:00.000+00:00",
                }
            ),
            self.make_response(
                text="<html>not json</html>",
                json_side_effect=ValueError("bad json"),
            ),
            self.make_response(
                payload={
                    "Persons": [{"PersonId": "person-1"}],
                    "HasMore": False,
                }
            ),
        ]
        sleep = Mock()
        client = self.make_client(session=session)
        client.sleep = sleep

        records = list(client.iter_appointment_reminder_records())

        self.assertEqual(records, [{"PersonId": "person-1"}])
        sleep.assert_called_once_with(30.0)

    def test_iter_appointment_reminder_records_treats_missing_persons_as_empty(self):
        session = Mock()
        session.request.side_effect = [
            self.make_response(
                payload={
                    "AccessToken": "access-token",
                    "Type": "Bearer",
                    "ExpiresAt": "9999-06-02T08:00:00.000+00:00",
                    "RefreshToken": "refresh-token",
                    "RefreshExpiresAt": "9999-06-09T06:00:00.000+00:00",
                }
            ),
            self.make_response(payload={"HasMore": False}),
        ]
        client = self.make_client(session=session)

        records = list(client.iter_appointment_reminder_records())

        self.assertEqual(records, [])

    def test_iter_appointment_reminder_records_rejects_non_list_persons(self):
        session = Mock()
        session.request.side_effect = [
            self.make_response(
                payload={
                    "AccessToken": "access-token",
                    "Type": "Bearer",
                    "ExpiresAt": "9999-06-02T08:00:00.000+00:00",
                    "RefreshToken": "refresh-token",
                    "RefreshExpiresAt": "9999-06-09T06:00:00.000+00:00",
                }
            ),
            self.make_response(payload={"Persons": {}, "HasMore": False}),
        ]
        client = self.make_client(session=session)

        with self.assertRaisesMessage(
            EDRWebAPIError,
            "EDRWeb Persons field must be a list.",
        ):
            list(client.iter_appointment_reminder_records())

    def test_iter_appointment_reminder_records_requires_boolean_has_more(self):
        for payload in (
            {"Persons": []},
            {"Persons": [], "HasMore": "false"},
        ):
            with self.subTest(payload=payload):
                session = Mock()
                session.request.side_effect = [
                    self.make_response(
                        payload={
                            "AccessToken": "access-token",
                            "Type": "Bearer",
                            "ExpiresAt": "9999-06-02T08:00:00.000+00:00",
                            "RefreshToken": "refresh-token",
                            "RefreshExpiresAt": "9999-06-09T06:00:00.000+00:00",
                        }
                    ),
                    self.make_response(payload=payload),
                ]
                client = self.make_client(session=session)

                with self.assertRaisesMessage(
                    EDRWebAPIError,
                    "EDRWeb HasMore field must be a boolean.",
                ):
                    list(client.iter_appointment_reminder_records())

    def test_iter_appointment_reminder_records_does_not_retry_bad_request_or_forbidden(
        self,
    ):
        for status_code in (400, 403):
            with self.subTest(status_code=status_code):
                session = Mock()
                session.request.side_effect = [
                    self.make_response(
                        payload={
                            "AccessToken": "access-token",
                            "Type": "Bearer",
                            "ExpiresAt": "9999-06-02T08:00:00.000+00:00",
                            "RefreshToken": "refresh-token",
                            "RefreshExpiresAt": "9999-06-09T06:00:00.000+00:00",
                        }
                    ),
                    self.make_response(status_code=status_code),
                ]
                sleep = Mock()
                client = self.make_client(session=session)
                client.sleep = sleep

                with self.assertRaisesMessage(
                    EDRWebAPIError,
                    f"EDRWeb request failed with status {status_code}",
                ):
                    list(client.iter_appointment_reminder_records())
                self.assertEqual(session.request.call_count, 2)
                sleep.assert_not_called()

    def test_iter_appointment_reminder_records_requires_cursor_when_has_more(self):
        session = Mock()
        session.request.side_effect = [
            self.make_response(
                payload={
                    "AccessToken": "access-token",
                    "Type": "Bearer",
                    "ExpiresAt": "9999-06-02T08:00:00.000+00:00",
                    "RefreshToken": "refresh-token",
                    "RefreshExpiresAt": "9999-06-09T06:00:00.000+00:00",
                }
            ),
            self.make_response(
                payload={
                    "Persons": [],
                    "HasMore": True,
                }
            ),
        ]
        client = self.make_client(session=session)

        with self.assertRaisesMessage(
            EDRWebAPIError,
            "EDRWeb NextCursor is required when HasMore is true.",
        ):
            list(client.iter_appointment_reminder_records())
