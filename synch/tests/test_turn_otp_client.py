from __future__ import annotations

from unittest.mock import Mock

import requests
from django.test import SimpleTestCase

from synch.turn import (
    OTP_REQUEST_TIMEOUT_SECONDS,
    TurnOTPAPIClient,
    TurnOTPTimeoutError,
    TurnOTPUpstreamError,
)

TEST_TOKEN = "test-token"  # noqa: S105


class TurnOTPAPIClientTests(SimpleTestCase):
    def make_client(self, session: Mock | None = None) -> TurnOTPAPIClient:
        client = TurnOTPAPIClient(
            base_url="https://whatsapp.turn.io",
            token=TEST_TOKEN,
            template_namespace="template-namespace",
            template_name="verification_code",
            template_language="en",
        )
        if session is not None:
            client.session = session
        return client

    def make_response(
        self,
        *,
        status_code: int = 200,
        json_payload: dict[str, object] | None = None,
    ) -> Mock:
        response = Mock()
        response.status_code = status_code
        response.headers = {}
        response.text = repr(json_payload)
        response.raise_for_status.side_effect = None
        response.json.return_value = json_payload or {}
        if status_code >= 400:
            response.raise_for_status.side_effect = requests.HTTPError(
                f"HTTP {status_code}",
                response=response,
            )
        return response

    def test_constructor_configures_json_headers(self) -> None:
        client = self.make_client()

        self.assertEqual(
            client.session.headers["Authorization"],
            f"Bearer {TEST_TOKEN}",
        )
        self.assertEqual(client.session.headers["Content-Type"], "application/json")
        self.assertEqual(
            client.session.headers["Accept"],
            "application/vnd.v1+json",
        )

    def test_send_authentication_template_message_posts_expected_payload(self) -> None:
        session = Mock()
        session.request.return_value = self.make_response(
            json_payload={"messages": [{"id": "wamid.123"}]}
        )
        client = self.make_client(session=session)

        message_id = client.send_authentication_template_message(
            msisdn="+27831234567",
            otp="493821",
        )

        self.assertEqual(message_id, "wamid.123")
        session.request.assert_called_once_with(
            method="POST",
            url="https://whatsapp.turn.io/v1/messages",
            json={
                "to": "+27831234567",
                "type": "template",
                "template": {
                    "namespace": "template-namespace",
                    "name": "verification_code",
                    "language": {
                        "code": "en",
                        "policy": "deterministic",
                    },
                    "components": [
                        {
                            "type": "body",
                            "parameters": [{"type": "text", "text": "493821"}],
                        },
                        {
                            "type": "button",
                            "sub_type": "url",
                            "index": "0",
                            "parameters": [{"type": "text", "text": "493821"}],
                        },
                    ],
                },
            },
            timeout=OTP_REQUEST_TIMEOUT_SECONDS,
        )

    def test_send_authentication_template_message_raises_timeout_error(self) -> None:
        session = Mock()
        session.request.side_effect = requests.Timeout("boom")
        client = self.make_client(session=session)

        with self.assertRaises(TurnOTPTimeoutError):
            client.send_authentication_template_message(
                msisdn="+27831234567",
                otp="493821",
            )

    def test_send_authentication_template_message_raises_for_http_failure(
        self,
    ) -> None:
        session = Mock()
        session.request.return_value = self.make_response(status_code=400)
        client = self.make_client(session=session)

        with self.assertRaises(TurnOTPUpstreamError):
            client.send_authentication_template_message(
                msisdn="+27831234567",
                otp="493821",
            )

    def test_send_authentication_template_message_raises_upstream_error_for_missing_id(
        self,
    ) -> None:
        session = Mock()
        session.request.return_value = self.make_response(json_payload={})
        client = self.make_client(session=session)

        with self.assertRaises(TurnOTPUpstreamError):
            client.send_authentication_template_message(
                msisdn="+27831234567",
                otp="493821",
            )
