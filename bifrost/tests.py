from importlib import reload
from unittest.mock import patch

from django.test import SimpleTestCase

import bifrost.settings.base as base_settings
from bifrost.phone_numbers import normalize_phone_number


class PhoneNumberTests(SimpleTestCase):
    def test_normalizes_local_south_african_phone_number_to_e164(self):
        self.assertEqual(normalize_phone_number("0820000001"), "+27820000001")

    def test_returns_none_for_unparseable_phone_number(self):
        self.assertIsNone(normalize_phone_number("not-a-phone-number"))


class SentryConfigurationTests(SimpleTestCase):
    def test_does_not_initialize_sentry_without_dsn(self):
        with (
            patch.dict("os.environ", {}, clear=True),
            patch("sentry_sdk.init") as init,
        ):
            reloaded_settings = reload(base_settings)

        init.assert_not_called()
        self.assertEqual(reloaded_settings.SENTRY_DSN, "")

    def test_initializes_sentry_when_dsn_is_set(self):
        with (
            patch.dict(
                "os.environ",
                {
                    "SENTRY_DSN": "https://public@example.ingest.sentry.io/1",
                },
                clear=True,
            ),
            patch("sentry_sdk.init") as init,
        ):
            reloaded_settings = reload(base_settings)

        self.assertEqual(
            reloaded_settings.SENTRY_DSN,
            "https://public@example.ingest.sentry.io/1",
        )
        init.assert_called_once_with(
            dsn="https://public@example.ingest.sentry.io/1",
        )
