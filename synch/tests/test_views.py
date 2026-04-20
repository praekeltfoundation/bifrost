from __future__ import annotations

from django.test import TestCase
from django.urls import reverse


class HealthTestCase(TestCase):
    def test_health(self):
        response = self.client.get(reverse("health"))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.text, "OK")
