from __future__ import annotations

from unittest.mock import Mock, call

import requests
from django.test import SimpleTestCase

from synch.turn import (
    REQUEST_TIMEOUT_SECONDS,
    RETRY_LIMIT,
    TURN_CONTACTS_CSV_MAX_BYTES,
    TurnAPIClient,
    TurnAPIError,
    TurnRetryExhausted,
    TurnRowTooLargeError,
)

TEST_TOKEN = "test-token"  # noqa: S105


class TurnAPIClientTests(SimpleTestCase):
    def make_client(
        self,
        session: Mock | None = None,
        sleep: Mock | None = None,
        random_uniform: Mock | None = None,
    ) -> TurnAPIClient:
        client = TurnAPIClient(
            base_url="https://whatsapp.turn.io",
            token=TEST_TOKEN,
        )
        if session:
            client.session = session
        if sleep:
            client.sleep = sleep
        if random_uniform:
            client.random_uniform = random_uniform
        return client

    def make_response(
        self,
        status_code: int = 200,
        text: str = "",
        headers: dict[str, str] | None = None,
    ) -> Mock:
        response = Mock()
        response.status_code = status_code
        response.text = text
        response.headers = headers or {}
        response.raise_for_status.side_effect = None
        if status_code >= 400:
            response.raise_for_status.side_effect = requests.HTTPError(
                f"HTTP {status_code}",
                response=response,
            )
        return response

    def test_constructor_configures_bearer_auth_and_default_headers(self):
        client = self.make_client()

        self.assertEqual(client.base_url, "https://whatsapp.turn.io")
        self.assertEqual(
            client.session.headers["Authorization"], f"Bearer {TEST_TOKEN}"
        )
        self.assertEqual(
            client.session.headers["Accept"],
            "application/vnd.v1+json",
        )
        self.assertEqual(client.session.headers["Content-Type"], "text/csv")

    def test_import_contacts_returns_empty_list_without_requests_for_empty_rows(self):
        session = Mock()
        client = self.make_client(session=session)

        errors = client.import_contacts([])

        self.assertEqual(errors, [])
        session.request.assert_not_called()

    def test_import_contacts_posts_csv_payload_with_union_of_fields(self):
        session = Mock()
        session.request.return_value = self.make_response(
            text="urn,name,surname,age\r\n"
        )
        client = self.make_client(session=session)

        errors = client.import_contacts(
            [
                {"urn": "+27123456789", "name": "Peter"},
                {"urn": "+27123456790", "surname": "Parker", "age": 21},
            ]
        )

        self.assertEqual(errors, [])
        session.request.assert_called_once_with(
            method="POST",
            url="https://whatsapp.turn.io/v1/contacts",
            data=(
                b"urn,name,surname,age\r\n"
                b"+27123456789,Peter,,\r\n"
                b"+27123456790,,Parker,21\r\n"
            ),
            timeout=REQUEST_TIMEOUT_SECONDS,
        )

    def test_import_contacts_requires_urn_column(self):
        client = self.make_client()

        with self.assertRaisesMessage(TurnAPIError, "urn"):
            client.import_contacts([{"name": "Peter"}])

    def test_import_contacts_returns_only_error_rows_from_csv_response(self):
        session = Mock()
        session.request.return_value = self.make_response(
            text=(
                "urn,name,opted_in\r\n"
                "+27123456789,Peter,true\r\n"
                "+27123456790,,ERROR: cannot cast value of 'yes' to boolean\r\n"
            )
        )
        client = self.make_client(session=session)

        errors = client.import_contacts(
            [
                {"urn": "+27123456789", "name": "Peter", "opted_in": "true"},
                {"urn": "+27123456790", "opted_in": "yes"},
            ]
        )

        self.assertEqual(
            errors,
            [
                {
                    "urn": "+27123456790",
                    "name": "",
                    "opted_in": "ERROR: cannot cast value of 'yes' to boolean",
                }
            ],
        )

    def test_import_contacts_retries_retry_after_as_minimum_delay(self):
        session = Mock()
        sleep = Mock()
        random_uniform = Mock(side_effect=[0.25, 0.1])
        session.request.side_effect = [
            self.make_response(status_code=429, headers={"Retry-After": "5"}),
            self.make_response(status_code=503),
            self.make_response(text="urn,name\r\n"),
        ]
        client = self.make_client(
            session=session,
            sleep=sleep,
            random_uniform=random_uniform,
        )

        errors = client.import_contacts([{"urn": "+27123456789", "name": "Peter"}])

        self.assertEqual(errors, [])
        random_uniform.assert_has_calls([call(0, 1), call(0, 2)])
        self.assertEqual(sleep.call_args_list, [call(5.0), call(0.1)])

    def test_import_contacts_retries_request_exceptions(self):
        session = Mock()
        sleep = Mock()
        random_uniform = Mock(return_value=0.25)
        session.request.side_effect = [
            requests.Timeout("boom"),
            self.make_response(text="urn,name\r\n"),
        ]
        client = self.make_client(
            session=session,
            sleep=sleep,
            random_uniform=random_uniform,
        )

        errors = client.import_contacts([{"urn": "+27123456789", "name": "Peter"}])

        self.assertEqual(errors, [])
        sleep.assert_called_once_with(0.25)

    def test_import_contacts_raises_after_retry_limit_exhausted(self):
        session = Mock()
        sleep = Mock()
        random_uniform = Mock(return_value=0.0)
        session.request.side_effect = [
            self.make_response(status_code=503) for _ in range(RETRY_LIMIT + 1)
        ]
        client = self.make_client(
            session=session,
            sleep=sleep,
            random_uniform=random_uniform,
        )

        with self.assertRaises(TurnRetryExhausted):
            client.import_contacts([{"urn": "+27123456789", "name": "Peter"}])

        self.assertEqual(sleep.call_count, RETRY_LIMIT)

    def test_import_contacts_raises_for_non_retryable_http_errors(self):
        session = Mock()
        session.request.return_value = self.make_response(status_code=400)
        client = self.make_client(session=session)

        with self.assertRaises(TurnAPIError):
            client.import_contacts([{"urn": "+27123456789", "name": "Peter"}])

    def test_import_contacts_splits_batches_under_size_limit(self):
        session = Mock()
        session.request.side_effect = [
            self.make_response(text="urn,name\r\n"),
            self.make_response(text="urn,name\r\n"),
        ]
        client = self.make_client(session=session)
        oversized_name = "x" * (TURN_CONTACTS_CSV_MAX_BYTES // 2)

        errors = client.import_contacts(
            [
                {"urn": "+27123456789", "name": oversized_name},
                {"urn": "+27123456790", "name": oversized_name},
            ]
        )

        self.assertEqual(errors, [])
        self.assertEqual(session.request.call_count, 2)
        for request_call in session.request.call_args_list:
            data = request_call.kwargs["data"]
            self.assertLessEqual(len(data), TURN_CONTACTS_CSV_MAX_BYTES)

    def test_import_contacts_raises_when_single_row_exceeds_batch_limit(self):
        client = self.make_client()
        oversized_name = "x" * TURN_CONTACTS_CSV_MAX_BYTES

        with self.assertRaises(TurnRowTooLargeError):
            client.import_contacts([{"urn": "+27123456789", "name": oversized_name}])
