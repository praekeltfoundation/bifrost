from __future__ import annotations

import json
from typing import Any, cast
from unittest.mock import Mock, patch

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Permission
from django.core.cache import caches
from django.test import TestCase, override_settings
from django.urls import reverse
from rest_framework.authtoken.models import Token
from rest_framework.test import APIClient

from synch.turn import TurnOTPTimeoutError, TurnOTPUpstreamError


@override_settings(
    TURN_BASE_URL="https://whatsapp.turn.io",
    TURN_OTP_TOKEN="otp-token",  # noqa: S106
    TURN_OTP_TEMPLATE_NAMESPACE="template-namespace",
    TURN_OTP_TEMPLATE_NAME="verification_code",
    TURN_OTP_TEMPLATE_LANGUAGE="en",
)
class SendOTPAPITests(TestCase):
    def setUp(self) -> None:
        self.api_client = cast(Any, APIClient())
        user_model = cast(Any, get_user_model())
        # These tests use token auth only; avoid paying default password hasher cost.
        self.user = user_model.objects.create_user(
            username="api-caller",
        )
        self.token = Token.objects.create(user=self.user)
        self.permission = Permission.objects.get(
            codename="send_otp",
            content_type__app_label="synch",
            content_type__model="apiuser",
        )
        caches["otp_delivery_throttle"].clear()

    def authenticate(self) -> None:
        self.api_client.credentials(HTTP_AUTHORIZATION=f"Token {self.token.key}")

    def grant_permission(self) -> None:
        self.user.user_permissions.add(self.permission)

    def test_requires_authentication(self) -> None:
        response = self.api_client.post(
            reverse("otp-send"),
            data={
                "msisdn": "+27831234567",
                "otp": "493821",
                "recipient_type": "patient",
            },
            format="json",
        )

        self.assertEqual(response.status_code, 401)

    def test_requires_permission(self) -> None:
        self.authenticate()

        response = self.api_client.post(
            reverse("otp-send"),
            data={
                "msisdn": "+27831234567",
                "otp": "493821",
                "recipient_type": "patient",
            },
            format="json",
        )

        self.assertEqual(response.status_code, 403)

    @patch("synch.api_views.get_turn_otp_client")
    def test_submits_valid_request_and_returns_message_id(
        self,
        get_turn_otp_client: Mock,
    ) -> None:
        self.authenticate()
        self.grant_permission()
        turn_client = Mock()
        turn_client.send_authentication_template_message.return_value = "wamid.123"
        get_turn_otp_client.return_value = turn_client

        response = self.api_client.post(
            reverse("otp-send"),
            data={
                "msisdn": "+27831234567",
                "otp": "493821",
                "recipient_type": "patient",
                "metadata": {"source": "qa"},
            },
            format="json",
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {"status": "submitted", "message_id": "wamid.123"},
        )
        turn_client.send_authentication_template_message.assert_called_once_with(
            msisdn="+27831234567",
            otp="493821",
        )

    def test_returns_drf_validation_errors(self) -> None:
        self.authenticate()
        self.grant_permission()

        response = self.api_client.post(
            reverse("otp-send"),
            data={
                "msisdn": "0831234567",
                "otp": "",
                "recipient_type": "invalid",
                "metadata": [],
            },
            format="json",
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.json(),
            {
                "msisdn": ["Enter a valid E.164 phone number."],
                "otp": ["This field may not be blank."],
                "recipient_type": ['"invalid" is not a valid choice.'],
                "metadata": ['Expected a dictionary of items but got type "list".'],
            },
        )

    def test_requires_recipient_type(self) -> None:
        self.authenticate()
        self.grant_permission()

        response = self.api_client.post(
            reverse("otp-send"),
            data={"msisdn": "+27831234567", "otp": "493821"},
            format="json",
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.json(), {"recipient_type": ["This field is required."]}
        )

    @patch("synch.api_views.get_turn_otp_client")
    def test_accepts_synch_user_recipient_type(
        self,
        get_turn_otp_client: Mock,
    ) -> None:
        self.authenticate()
        self.grant_permission()
        turn_client = Mock()
        turn_client.send_authentication_template_message.return_value = "wamid.123"
        get_turn_otp_client.return_value = turn_client

        response = self.api_client.post(
            reverse("otp-send"),
            data={
                "msisdn": "+27831234567",
                "otp": "493821",
                "recipient_type": "synch_user",
            },
            format="json",
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {"status": "submitted", "message_id": "wamid.123"},
        )

    @override_settings(
        REST_FRAMEWORK={
            "DEFAULT_AUTHENTICATION_CLASSES": [
                "rest_framework.authentication.TokenAuthentication",
            ],
            "DEFAULT_THROTTLE_RATES": {"otp_delivery": "2/h"},
            "DEFAULT_SCHEMA_CLASS": "drf_spectacular.openapi.AutoSchema",
        }
    )
    @patch("synch.api_views.get_turn_otp_client")
    def test_throttles_by_turn_contact(
        self,
        get_turn_otp_client: Mock,
    ) -> None:
        self.authenticate()
        self.grant_permission()
        turn_client = Mock()
        turn_client.send_authentication_template_message.return_value = "wamid.123"
        get_turn_otp_client.return_value = turn_client

        for _ in range(2):
            response = self.api_client.post(
                reverse("otp-send"),
                data={
                    "msisdn": "+27831234567",
                    "otp": "493821",
                    "recipient_type": "patient",
                },
                format="json",
            )
            self.assertEqual(response.status_code, 200)

        response = self.api_client.post(
            reverse("otp-send"),
            data={
                "msisdn": "+27831234567",
                "otp": "493821",
                "recipient_type": "synch_user",
            },
            format="json",
        )

        self.assertEqual(response.status_code, 429)

    @override_settings(
        REST_FRAMEWORK={
            "DEFAULT_AUTHENTICATION_CLASSES": [
                "rest_framework.authentication.TokenAuthentication",
            ],
            "DEFAULT_THROTTLE_RATES": {"otp_delivery": "1/h"},
            "DEFAULT_SCHEMA_CLASS": "drf_spectacular.openapi.AutoSchema",
        }
    )
    def test_invalid_msisdn_does_not_consume_throttle_budget(self) -> None:
        self.authenticate()
        self.grant_permission()

        for _ in range(2):
            response = self.api_client.post(
                reverse("otp-send"),
                data={
                    "msisdn": "0831234567",
                    "otp": "493821",
                    "recipient_type": "patient",
                },
                format="json",
            )
            self.assertEqual(response.status_code, 400)

    @patch("synch.api_views.sentry_sdk.capture_exception")
    @patch("synch.api_views.get_turn_otp_client")
    def test_returns_502_for_upstream_provider_failures(
        self,
        get_turn_otp_client: Mock,
        capture_exception: Mock,
    ) -> None:
        self.authenticate()
        self.grant_permission()
        turn_client = Mock()
        turn_client.send_authentication_template_message.side_effect = (
            TurnOTPUpstreamError("boom")
        )
        get_turn_otp_client.return_value = turn_client

        response = self.api_client.post(
            reverse("otp-send"),
            data={
                "msisdn": "+27831234567",
                "otp": "493821",
                "recipient_type": "patient",
            },
            format="json",
        )

        self.assertEqual(response.status_code, 502)
        self.assertEqual(
            response.json(),
            {"detail": "Upstream messaging provider error."},
        )
        capture_exception.assert_called_once()

    @patch("synch.api_views.sentry_sdk.capture_exception")
    @patch("synch.api_views.get_turn_otp_client")
    def test_returns_504_for_upstream_timeouts(
        self,
        get_turn_otp_client: Mock,
        capture_exception: Mock,
    ) -> None:
        self.authenticate()
        self.grant_permission()
        turn_client = Mock()
        turn_client.send_authentication_template_message.side_effect = (
            TurnOTPTimeoutError("boom")
        )
        get_turn_otp_client.return_value = turn_client

        response = self.api_client.post(
            reverse("otp-send"),
            data={
                "msisdn": "+27831234567",
                "otp": "493821",
                "recipient_type": "patient",
            },
            format="json",
        )

        self.assertEqual(response.status_code, 504)
        self.assertEqual(
            response.json(),
            {
                "detail": (
                    "Timed out waiting for the messaging provider. "
                    "Delivery outcome is unknown."
                )
            },
        )
        capture_exception.assert_called_once()


class APIDocumentationTests(TestCase):
    def test_schema_endpoint_includes_send_otp_path(self) -> None:
        response = self.client.get(f"{reverse('api-schema')}?format=json")

        self.assertEqual(response.status_code, 200)
        schema = json.loads(response.content)
        self.assertIn("/api/v1/otp/send/", schema["paths"])
        security_schemes = schema["components"]["securitySchemes"]
        self.assertEqual(set(security_schemes), {"tokenAuth"})
        self.assertEqual(security_schemes["tokenAuth"]["type"], "apiKey")
        self.assertEqual(security_schemes["tokenAuth"]["in"], "header")
        self.assertEqual(security_schemes["tokenAuth"]["name"], "Authorization")
        request_schema = schema["components"]["schemas"]["SendOTPRequest"]
        self.assertEqual(
            request_schema["properties"]["metadata"]["description"],
            "Optional metadata reserved for future use. Accepted but ignored.",
        )
        self.assertEqual(
            request_schema["required"],
            ["msisdn", "otp", "recipient_type"],
        )
        self.assertEqual(
            request_schema["properties"]["recipient_type"]["allOf"],
            [{"$ref": "#/components/schemas/RecipientTypeEnum"}],
        )
        self.assertEqual(
            schema["components"]["schemas"]["RecipientTypeEnum"]["enum"],
            ["patient", "synch_user"],
        )

    def test_swagger_ui_endpoint_is_public(self) -> None:
        response = self.client.get(reverse("api-docs"))

        self.assertEqual(response.status_code, 200)
