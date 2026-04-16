from django.test import TestCase, override_settings
from django.urls import reverse
from django.utils.module_loading import import_string

from bifrost.celery import app
from synch.tasks import healthcheck


class HealthTestCase(TestCase):
    def test_health(self):
        response = self.client.get(reverse("health"))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.text, "OK")


class CeleryConfigurationTests(TestCase):
    def test_uses_amqp_broker_by_default(self):
        self.assertEqual(app.conf.broker_url, "amqp://guest:guest@localhost:5672//")

    def test_does_not_configure_result_backend(self):
        self.assertIsNone(app.conf.result_backend)

    def test_autodiscovers_shared_tasks_from_django_apps(self):
        task = import_string("synch.tasks.healthcheck")

        self.assertIn(task.name, app.tasks)
        self.assertEqual(app.tasks[task.name].name, task.name)


@override_settings(
    CELERY_TASK_ALWAYS_EAGER=True,
    CELERY_TASK_EAGER_PROPAGATES=True,
)
class CeleryTaskExecutionTests(TestCase):
    def test_healthcheck_task_runs(self):
        result = healthcheck.delay()

        self.assertTrue(result.successful())
        self.assertEqual(result.get(), "OK")
