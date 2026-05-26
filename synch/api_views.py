from __future__ import annotations

import logging
from typing import cast

import sentry_sdk
from django.conf import settings
from django.contrib.auth.models import User
from django.core.cache import caches
from django.db.models import TextChoices
from drf_spectacular.utils import (
    OpenApiExample,
    OpenApiResponse,
    extend_schema,
    extend_schema_view,
)
from rest_framework import serializers, status
from rest_framework.decorators import action
from rest_framework.parsers import JSONParser
from rest_framework.permissions import BasePermission
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.settings import api_settings
from rest_framework.throttling import SimpleRateThrottle
from rest_framework.views import APIView
from rest_framework.viewsets import GenericViewSet

from synch.phone_numbers import normalize_e164_phone_number
from synch.turn import TurnOTPAPIClient, TurnOTPTimeoutError, TurnOTPUpstreamError

logger = logging.getLogger(__name__)


class OTPRecipientType(TextChoices):
    PATIENT = "patient", "Patient"
    SYNCH_USER = "synch_user", "SyNCH user"


def get_turn_otp_client() -> TurnOTPAPIClient:
    return TurnOTPAPIClient(
        base_url=settings.TURN_BASE_URL,
        token=settings.TURN_OTP_TOKEN,
        template_namespace=settings.TURN_OTP_TEMPLATE_NAMESPACE,
        template_name=settings.TURN_OTP_TEMPLATE_NAME,
        template_language=settings.TURN_OTP_TEMPLATE_LANGUAGE,
    )


class HasSendOTPPermission(BasePermission):
    def has_permission(self, request: Request, view: APIView) -> bool:
        user = request.user
        if not user or not user.is_authenticated:
            return False
        return cast(User, user).has_perm("synch.send_otp")


class OTPDeliveryThrottle(SimpleRateThrottle):
    cache = caches["otp_delivery_throttle"]
    scope = "otp_delivery"

    def __init__(self) -> None:
        self.THROTTLE_RATES = cast(
            dict[str, str | None], api_settings.DEFAULT_THROTTLE_RATES
        )
        super().__init__()

    def get_cache_key(self, request: Request, view: APIView) -> str | None:
        msisdn = request.data.get("msisdn")
        if not isinstance(msisdn, str):
            return None

        normalized_msisdn = normalize_e164_phone_number(msisdn)
        if normalized_msisdn is None:
            return None

        return f"otp-delivery-throttle:{normalized_msisdn}"


class SendOTPRequestSerializer(serializers.Serializer):
    msisdn = serializers.CharField(
        help_text="Recipient WhatsApp number in E.164 format.",
    )
    otp = serializers.CharField(
        max_length=15,
        allow_blank=False,
        trim_whitespace=False,
        help_text="OTP code to include in the template message.",
    )
    recipient_type = serializers.ChoiceField(
        choices=OTPRecipientType.choices,
        help_text=(
            "Business-role classification for this OTP recipient. Required but "
            "does not change delivery behavior in the current version."
        ),
    )
    metadata = serializers.DictField(
        child=serializers.JSONField(),
        required=False,
        help_text="Optional metadata reserved for future use. Accepted but ignored.",
        default=dict,
    )

    def validate_msisdn(self, value: str) -> str:
        normalized_phone_number = normalize_e164_phone_number(value)
        if normalized_phone_number is None:
            raise serializers.ValidationError("Enter a valid E.164 phone number.")
        return normalized_phone_number

    def validate_metadata(self, value: object) -> dict[str, object]:
        if not isinstance(value, dict):
            raise serializers.ValidationError("Expected an object of key-value pairs.")
        return value


class SendOTPResponseSerializer(serializers.Serializer):
    status = serializers.CharField()
    message_id = serializers.CharField()


class ErrorDetailSerializer(serializers.Serializer):
    detail = serializers.CharField(help_text="Human-readable error message.")


class ValidationErrorResponseSerializer(serializers.Serializer):
    msisdn = serializers.ListField(
        child=serializers.CharField(),
        required=False,
        help_text="Validation messages for msisdn.",
    )
    otp = serializers.ListField(
        child=serializers.CharField(),
        required=False,
        help_text="Validation messages for otp.",
    )
    recipient_type = serializers.ListField(
        child=serializers.CharField(),
        required=False,
        help_text="Validation messages for recipient_type.",
    )
    metadata = serializers.ListField(
        child=serializers.CharField(),
        required=False,
        help_text="Validation messages for metadata.",
    )


@extend_schema_view(
    send=extend_schema(
        request=SendOTPRequestSerializer,
        responses={
            200: OpenApiResponse(
                response=SendOTPResponseSerializer,
                description="The messaging provider accepted the OTP template message.",
                examples=[
                    OpenApiExample(
                        "OTP delivery accepted",
                        value={"status": "submitted", "message_id": "wamid.123"},
                        response_only=True,
                    )
                ],
            ),
            400: OpenApiResponse(
                response=ValidationErrorResponseSerializer,
                description="The request body failed validation.",
                examples=[
                    OpenApiExample(
                        "Validation error",
                        value={
                            "msisdn": ["Enter a valid E.164 phone number."],
                            "otp": ["This field may not be blank."],
                            "recipient_type": ['"invalid" is not a valid choice.'],
                            "metadata": ["Expected an object of key-value pairs."],
                        },
                        response_only=True,
                    )
                ],
            ),
            401: OpenApiResponse(
                response=ErrorDetailSerializer,
                description="Authentication credentials were missing or invalid.",
                examples=[
                    OpenApiExample(
                        "Authentication required",
                        value={
                            "detail": "Authentication credentials were not provided."
                        },
                        response_only=True,
                    )
                ],
            ),
            403: OpenApiResponse(
                response=ErrorDetailSerializer,
                description="The authenticated caller lacks the send OTP permission.",
                examples=[
                    OpenApiExample(
                        "Permission denied",
                        value={
                            "detail": (
                                "You do not have permission to perform this action."
                            )
                        },
                        response_only=True,
                    )
                ],
            ),
            429: OpenApiResponse(
                response=ErrorDetailSerializer,
                description=(
                    "Delivery protection blocked too many OTP send attempts to the "
                    "same Turn Contact within the configured throttle window."
                ),
                examples=[
                    OpenApiExample(
                        "Delivery protection",
                        value={
                            "detail": (
                                "Request was throttled. Expected available in "
                                "3600 seconds."
                            )
                        },
                        response_only=True,
                    )
                ],
            ),
            502: OpenApiResponse(
                response=ErrorDetailSerializer,
                description=(
                    "The upstream messaging provider returned a non-timeout failure."
                ),
                examples=[
                    OpenApiExample(
                        "Upstream failure",
                        value={"detail": "Upstream messaging provider error."},
                        response_only=True,
                    )
                ],
            ),
            504: OpenApiResponse(
                response=ErrorDetailSerializer,
                description=(
                    "Timed out waiting for the upstream messaging provider. Delivery "
                    "outcome is unknown, so retries may duplicate delivery."
                ),
                examples=[
                    OpenApiExample(
                        "Upstream timeout",
                        value={
                            "detail": (
                                "Timed out waiting for the messaging provider. "
                                "Delivery outcome is unknown."
                            )
                        },
                        response_only=True,
                    )
                ],
            ),
        },
        examples=[
            OpenApiExample(
                "OTP delivery request",
                value={
                    "msisdn": "+27831234567",
                    "otp": "493821",
                    "recipient_type": "patient",
                    "metadata": {"source": "sync-qa"},
                },
                request_only=True,
            ),
        ],
        description=(
            "Sends a WhatsApp authentication template message to the submitted "
            "Turn Contact. The caller owns OTP generation, expiry, validation, "
            "and resend policy. Temporary upstream failures may have unknown "
            "delivery outcome, so retries can duplicate delivery."
        ),
    )
)
class SendOTPViewSet(GenericViewSet):
    permission_classes = (HasSendOTPPermission,)
    throttle_classes = (OTPDeliveryThrottle,)
    parser_classes = (JSONParser,)
    serializer_class = SendOTPRequestSerializer

    @action(detail=False, methods=["post"], url_path="send")
    def send(self, request: Request) -> Response:
        serializer = SendOTPRequestSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        msisdn = serializer.validated_data["msisdn"]
        otp = serializer.validated_data["otp"]
        recipient_type = serializer.validated_data["recipient_type"]

        try:
            message_id = get_turn_otp_client().send_authentication_template_message(
                msisdn=msisdn,
                otp=otp,
            )
        except TurnOTPTimeoutError as error:
            sentry_sdk.capture_exception(error)
            logger.warning(
                "Timed out sending %s OTP template to %s for API caller %s.",
                recipient_type,
                msisdn,
                request.user.get_username(),
            )
            return Response(
                {
                    "detail": (
                        "Timed out waiting for the messaging provider. "
                        "Delivery outcome is unknown."
                    )
                },
                status=status.HTTP_504_GATEWAY_TIMEOUT,
            )
        except TurnOTPUpstreamError as error:
            sentry_sdk.capture_exception(error)
            logger.warning(
                "Upstream %s OTP send failure for %s from API caller %s.",
                recipient_type,
                msisdn,
                request.user.get_username(),
            )
            return Response(
                {"detail": "Upstream messaging provider error."},
                status=status.HTTP_502_BAD_GATEWAY,
            )

        logger.info(
            "Submitted %s OTP template message %s to %s for API caller %s.",
            recipient_type,
            message_id,
            msisdn,
            request.user.get_username(),
        )
        return Response(
            {"status": "submitted", "message_id": message_id},
            status=status.HTTP_200_OK,
        )
