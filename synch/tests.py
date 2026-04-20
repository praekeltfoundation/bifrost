from __future__ import annotations

from datetime import datetime
from unittest.mock import Mock, call

import requests
from django.test import SimpleTestCase, TestCase, override_settings
from django.urls import reverse
from django.utils.module_loading import import_string

from bifrost.celery import app
from synch.ccmdd import (
    LONG_RUNNING_POLL_INTERVAL_SECONDS,
    LONG_RUNNING_STATUS_POLL_LIMIT,
    REQUEST_TIMEOUT_SECONDS,
    CCMDDAPIClient,
    CCMDDAPIError,
    CCMDDLongRunningOperationTimeout,
)
from synch.tasks import healthcheck

TEST_PASSWORD = "test-password"  # noqa: S105


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


class CCMDDAPIClientTests(SimpleTestCase):
    def make_client(
        self,
        session: Mock | None = None,
        sleep: Mock | None = None,
        random_uniform: Mock | None = None,
    ) -> CCMDDAPIClient:
        client = CCMDDAPIClient(
            base_url="https://test.ccmdd.org.za",
            username="api-user",
            password=TEST_PASSWORD,
        )
        if session:
            client.session = session
        if sleep:
            client.sleep = sleep or Mock()
        if random_uniform:
            client.random_uniform = random_uniform or Mock()
        return client

    def make_response(
        self,
        status_code: int = 200,
        payload: dict | None = None,
    ) -> Mock:
        response = Mock()
        response.status_code = status_code
        response.json.return_value = payload if payload is not None else {}
        response.headers = {}
        response.text = ""
        response.raise_for_status.side_effect = None
        if status_code >= 400:
            response.raise_for_status.side_effect = requests.HTTPError(
                f"HTTP {status_code}",
                response=response,
            )
        return response

    def test_constructor_configures_digest_auth_on_session(self):
        client = self.make_client()

        self.assertEqual(client.base_url, "https://test.ccmdd.org.za")
        self.assertEqual(client.session.auth.username, "api-user")
        self.assertEqual(client.session.auth.password, TEST_PASSWORD)

    def test_iter_limited_prescriptions_posts_without_date_updated_when_unset(self):
        session = Mock()
        session.request.return_value = self.make_response(
            payload={"result": 1, "data": [{"id": "rx-1"}]},
        )
        client = self.make_client(session=session)

        items = list(client.iter_limited_prescriptions())

        self.assertEqual(items, [{"id": "rx-1"}])
        session.request.assert_called_once_with(
            method="POST",
            url="https://test.ccmdd.org.za/wapi/prescriptionLimited",
            json={},
            timeout=REQUEST_TIMEOUT_SECONDS,
        )

    def test_iter_limited_patients_posts_with_date_updated_when_provided(self):
        session = Mock()
        session.request.return_value = self.make_response(
            payload={"result": 1, "data": [{"id": "patient-1"}]},
        )
        client = self.make_client(session=session)

        items = list(
            client.iter_limited_patients(
                date_updated=datetime(2024, 1, 2, 3, 4, 5),
            ),
        )

        self.assertEqual(items, [{"id": "patient-1"}])
        session.request.assert_called_once_with(
            method="POST",
            url="https://test.ccmdd.org.za/wapi/patientLimited",
            json={"date_updated": "2024-01-02 03:04:05.000000"},
            timeout=REQUEST_TIMEOUT_SECONDS,
        )

    def test_iter_limited_prescriptions_waits_for_single_long_running_operation(self):
        session = Mock()
        sleep = Mock()
        session.request.side_effect = [
            self.make_response(
                status_code=202,
                payload={
                    "result": 2,
                    "response": {
                        "status_location": "https://status/1",
                        "resource_location": "https://resource/1",
                    },
                },
            ),
            self.make_response(
                payload={"result": 1, "data": {"status": "running"}},
            ),
            self.make_response(
                payload={"result": 1, "data": {"status": "succeeded"}},
            ),
            self.make_response(
                payload={"result": 1, "data": [{"id": "rx-1"}, {"id": "rx-2"}]},
            ),
        ]
        client = self.make_client(session=session, sleep=sleep)

        items = list(client.iter_limited_prescriptions())

        self.assertEqual(items, [{"id": "rx-1"}, {"id": "rx-2"}])
        self.assertEqual(
            sleep.call_args_list,
            [
                call(LONG_RUNNING_POLL_INTERVAL_SECONDS),
                call(LONG_RUNNING_POLL_INTERVAL_SECONDS),
            ],
        )
        self.assertEqual(
            session.request.call_args_list,
            [
                call(
                    method="POST",
                    url="https://test.ccmdd.org.za/wapi/prescriptionLimited",
                    json={},
                    timeout=REQUEST_TIMEOUT_SECONDS,
                ),
                call(
                    method="GET",
                    url="https://status/1",
                    json=None,
                    timeout=REQUEST_TIMEOUT_SECONDS,
                ),
                call(
                    method="GET",
                    url="https://status/1",
                    json=None,
                    timeout=REQUEST_TIMEOUT_SECONDS,
                ),
                call(
                    method="GET",
                    url="https://resource/1",
                    json=None,
                    timeout=REQUEST_TIMEOUT_SECONDS,
                ),
            ],
        )

    def test_iter_limited_patients_flattens_multi_long_running_operations(self):
        session = Mock()
        sleep = Mock()
        session.request.side_effect = [
            self.make_response(
                status_code=202,
                payload={
                    "result": 3,
                    "responses": [
                        {
                            "status_location": "https://status/1",
                            "resource_location": "https://resource/1",
                        },
                        {
                            "status_location": "https://status/2",
                            "resource_location": "https://resource/2",
                        },
                    ],
                },
            ),
            self.make_response(
                payload={"result": 1, "data": {"status": "succeeded"}},
            ),
            self.make_response(
                payload={"result": 1, "data": [{"id": "patient-1"}]},
            ),
            self.make_response(
                payload={"result": 1, "data": {"status": "succeeded"}},
            ),
            self.make_response(
                payload={"result": 1, "data": [{"id": "patient-2"}]},
            ),
        ]
        client = self.make_client(session=session, sleep=sleep)

        items = list(client.iter_limited_patients())

        self.assertEqual(items, [{"id": "patient-1"}, {"id": "patient-2"}])
        self.assertEqual(
            sleep.call_args_list,
            [
                call(LONG_RUNNING_POLL_INTERVAL_SECONDS),
                call(LONG_RUNNING_POLL_INTERVAL_SECONDS),
            ],
        )

    def test_status_poll_limit_only_counts_logical_status_polls(self):
        session = Mock()
        sleep = Mock()
        random_uniform = Mock(return_value=0.0)
        session.request.side_effect = [
            self.make_response(
                status_code=202,
                payload={
                    "result": 2,
                    "response": {
                        "status_location": "https://status/1",
                        "resource_location": "https://resource/1",
                    },
                },
            ),
            self.make_response(status_code=503),
            self.make_response(
                payload={"result": 1, "data": {"status": "running"}},
            ),
            *[
                self.make_response(
                    payload={"result": 1, "data": {"status": "running"}},
                )
                for _ in range(LONG_RUNNING_STATUS_POLL_LIMIT - 1)
            ],
        ]
        client = self.make_client(
            session=session,
            sleep=sleep,
            random_uniform=random_uniform,
        )

        with self.assertRaises(CCMDDLongRunningOperationTimeout):
            list(client.iter_limited_prescriptions())

        status_calls = [
            request_call
            for request_call in session.request.call_args_list
            if request_call.kwargs["url"] == "https://status/1"
        ]
        self.assertEqual(len(status_calls), LONG_RUNNING_STATUS_POLL_LIMIT + 1)
        self.assertEqual(sleep.call_count, LONG_RUNNING_STATUS_POLL_LIMIT + 1)

    def test_retries_temporary_http_failures_with_random_exponential_delay(self):
        session = Mock()
        sleep = Mock()
        random_uniform = Mock(side_effect=[0.25, 0.75])
        session.request.side_effect = [
            self.make_response(status_code=429),
            self.make_response(status_code=503),
            self.make_response(payload={"result": 1, "data": [{"id": "rx-1"}]}),
        ]
        client = self.make_client(
            session=session,
            sleep=sleep,
            random_uniform=random_uniform,
        )

        items = list(client.iter_limited_prescriptions())

        self.assertEqual(items, [{"id": "rx-1"}])
        random_uniform.assert_has_calls([call(0, 1), call(0, 2)])
        self.assertEqual(sleep.call_args_list, [call(0.25), call(0.75)])

    def test_retries_transport_errors(self):
        session = Mock()
        sleep = Mock()
        random_uniform = Mock(return_value=0.5)
        session.request.side_effect = [
            requests.ConnectionError("temporary"),
            self.make_response(payload={"result": 1, "data": [{"id": "rx-1"}]}),
        ]
        client = self.make_client(
            session=session,
            sleep=sleep,
            random_uniform=random_uniform,
        )

        items = list(client.iter_limited_prescriptions())

        self.assertEqual(items, [{"id": "rx-1"}])
        sleep.assert_called_once_with(0.5)

    def test_does_not_retry_non_temporary_http_failures(self):
        session = Mock()
        session.request.return_value = self.make_response(status_code=400)
        client = self.make_client(session=session)

        with self.assertRaises(CCMDDAPIError):
            list(client.iter_limited_prescriptions())

        session.request.assert_called_once()

    def test_raises_when_long_running_operation_does_not_succeed_in_time(self):
        session = Mock()
        sleep = Mock()
        session.request.side_effect = [
            self.make_response(
                status_code=202,
                payload={
                    "result": 2,
                    "response": {
                        "status_location": "https://status/1",
                        "resource_location": "https://resource/1",
                    },
                },
            ),
            *[
                self.make_response(
                    payload={"result": 1, "data": {"status": "running"}},
                )
                for _ in range(LONG_RUNNING_STATUS_POLL_LIMIT)
            ],
        ]
        client = self.make_client(session=session, sleep=sleep)

        with self.assertRaises(CCMDDLongRunningOperationTimeout):
            list(client.iter_limited_prescriptions())

        self.assertEqual(sleep.call_count, LONG_RUNNING_STATUS_POLL_LIMIT)
