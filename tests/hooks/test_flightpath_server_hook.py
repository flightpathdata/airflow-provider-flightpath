from __future__ import annotations

import unittest
from unittest.mock import patch, MagicMock

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.utils import db

from airflow.providers.flightpath_server.hooks.flightpath_server import FlightPathServerHook


class TestFlightPathServerHook(unittest.TestCase):
    def setUp(self):
        super().setUp()
        db.merge_conn(
            Connection(
                conn_id="flightpath_server_default",
                conn_type="flightpath_server",
                host="http://localhost:8000",
                password="test_api_key",
            )
        )

    @patch("requests.post")
    @patch("airflow.hooks.base.BaseHook.get_connection")
    def test_register_file(self, mock_get_connection, mock_requests_post):
        mock_get_connection.return_value = Connection(
            conn_id="flightpath_server_default",
            conn_type="flightpath_server",
            host="http://localhost:8000",
            password="test_api_key",
        )
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"reference": "test_ref_123"}
        mock_response.raise_for_status.return_value = None
        mock_requests_post.return_value = mock_response

        hook = FlightPathServerHook()
        response = hook.register_file(
            project_name="test_project",
            name="test_file.csv",
            file_location="/data/test_file.csv",
        )

        mock_requests_post.assert_called_once_with(
            "http://localhost:8000/csvpath/register_file",
            headers={
                "access_token": "test_api_key",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            data='{"project_name": "test_project", "name": "test_file.csv", "file_location": "/data/test_file.csv"}',
        )
        self.assertEqual(response, {"reference": "test_ref_123"})

    @patch("requests.post")
    @patch("airflow.hooks.base.BaseHook.get_connection")
    def test_register_and_run(self, mock_get_connection, mock_requests_post):
        mock_get_connection.return_value = Connection(
            conn_id="flightpath_server_default",
            conn_type="flightpath_server",
            host="http://localhost:8000",
            password="test_api_key",
        )
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "message": "success",
            "run_reference": "run_ref_456",
            "register_reference": "reg_ref_789",
        }
        mock_response.raise_for_status.return_value = None
        mock_requests_post.return_value = mock_response

        hook = FlightPathServerHook()
        response = hook.register_and_run(
            project_name="test_project",
            file_location="/data/new_file.jsonl",
            file_name="new_file.jsonl",
            csvpaths_group_name="my_pipeline",
        )

        mock_requests_post.assert_called_once_with(
            "http://localhost:8000/csvpath/register_and_run",
            headers={
                "access_token": "test_api_key",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            data='{"project_name": "test_project", "file_location": "/data/new_file.jsonl", "file_name": "new_file.jsonl", "csvpaths_group_name": "my_pipeline", "method": "collect_paths"}',
        )
        self.assertEqual(
            response,
            {
                "message": "success",
                "run_reference": "run_ref_456",
                "register_reference": "reg_ref_789",
            },
        )

    @patch("requests.post")
    @patch("airflow.hooks.base.BaseHook.get_connection")
    def test_get_file(self, mock_get_connection, mock_requests_post):
        mock_get_connection.return_value = Connection(
            conn_id="flightpath_server_default",
            conn_type="flightpath_server",
            host="http://localhost:8000",
            password="test_api_key",
        )
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"file": "YmFzZTY0IGVuY29kZWQgY29udGVudA=="} # base64 encoded "base64 encoded content"
        mock_response.raise_for_status.return_value = None
        mock_requests_post.return_value = mock_response

        hook = FlightPathServerHook()
        response = hook.get_file(
            project_name="test_project",
            reference="file_ref_xyz",
        )

        mock_requests_post.assert_called_once_with(
            "http://localhost:8000/find/get_file",
            headers={
                "access_token": "test_api_key",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            data='{"project_name": "test_project", "reference": "file_ref_xyz"}',
        )
        self.assertEqual(response, {"file": "YmFzZTY0IGVuY29kZWQgY29udGVudA=="})

    @patch("requests.post")
    @patch("airflow.hooks.base.BaseHook.get_connection")
    def test_api_call_failure(self, mock_get_connection, mock_requests_post):
        mock_get_connection.return_value = Connection(
            conn_id="flightpath_server_default",
            conn_type="flightpath_server",
            host="http://localhost:8000",
            password="test_api_key",
        )
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("Bad Request")
        mock_requests_post.return_value = mock_response

        hook = FlightPathServerHook()
        with self.assertRaisesRegex(AirflowException, "FlightPath Server API call failed"):
            hook.register_file(
                project_name="test_project",
                name="test_file.csv",
                file_location="/data/test_file.csv",
            )

    @patch("requests.post")
    @patch("airflow.hooks.base.BaseHook.get_connection")
    def test_api_call_json_decode_error(self, mock_get_connection, mock_requests_post):
        mock_get_connection.return_value = Connection(
            conn_id="flightpath_server_default",
            conn_type="flightpath_server",
            host="http://localhost:8000",
            password="test_api_key",
        )
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.side_effect = json.JSONDecodeError("Invalid JSON", "", 0)
        mock_response.text = "Invalid JSON response"
        mock_response.raise_for_status.return_value = None
        mock_requests_post.return_value = mock_response

        hook = FlightPathServerHook()
        with self.assertRaisesRegex(AirflowException, "Failed to decode JSON response from FlightPath Server"):
            hook.register_file(
                project_name="test_project",
                name="test_file.csv",
                file_location="/data/test_file.csv",
            )
