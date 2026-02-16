from __future__ import annotations

import unittest
from unittest.mock import patch, MagicMock
import base64
import os

from airflow.providers.flightpath_server.operators.flightpath_server import (
    FlightPathServerRegisterFileOperator,
    FlightPathServerRegisterAndRunOperator,
    FlightPathServerPullDataOperator,
)


class TestFlightPathServerRegisterFileOperator(unittest.TestCase):
    @patch("airflow.providers.flightpath_server.hooks.flightpath_server.FlightPathServerHook.register_file")
    def test_execute(self, mock_register_file):
        mock_register_file.return_value = {"reference": "test_ref_123"}

        operator = FlightPathServerRegisterFileOperator(
            task_id="test_register_file",
            project_name="test_project",
            name="test_file.csv",
            file_location="/data/test_file.csv",
        )
        response = operator.execute(context={})

        mock_register_file.assert_called_once_with(
            project_name="test_project",
            name="test_file.csv",
            file_location="/data/test_file.csv",
            template=None,
        )
        self.assertEqual(response, {"reference": "test_ref_123"})


class TestFlightPathServerRegisterAndRunOperator(unittest.TestCase):
    @patch("airflow.providers.flightpath_server.hooks.flightpath_server.FlightPathServerHook.register_and_run")
    def test_execute(self, mock_register_and_run):
        mock_register_and_run.return_value = {
            "message": "success",
            "run_reference": "run_ref_456",
            "register_reference": "reg_ref_789",
        }

        operator = FlightPathServerRegisterAndRunOperator(
            task_id="test_register_and_run",
            project_name="test_project",
            file_location="/data/new_file.jsonl",
            file_name="new_file.jsonl",
            csvpaths_group_name="my_pipeline",
        )
        response = operator.execute(context={})

        mock_register_and_run.assert_called_once_with(
            project_name="test_project",
            file_location="/data/new_file.jsonl",
            file_name="new_file.jsonl",
            csvpaths_group_name="my_pipeline",
            method="collect_paths",
            file_template=None,
            run_template=None,
        )
        self.assertEqual(
            response,
            {
                "message": "success",
                "run_reference": "run_ref_456",
                "register_reference": "reg_ref_789",
            },
        )


class TestFlightPathServerPullDataOperator(unittest.TestCase):
    @patch("airflow.providers.flightpath_server.hooks.flightpath_server.FlightPathServerHook.get_file")
    def test_execute(self, mock_get_file):
        test_content = "This is some test content."
        encoded_content = base64.b64encode(test_content.encode("utf-8")).decode("utf-8")
        mock_get_file.return_value = {"file": encoded_content}

        output_path = "/tmp/test_output.txt"
        if os.path.exists(output_path):
            os.remove(output_path)

        operator = FlightPathServerPullDataOperator(
            task_id="test_pull_data",
            project_name="test_project",
            reference="file_ref_xyz",
            output_path=output_path,
        )
        returned_path = operator.execute(context={})

        mock_get_file.assert_called_once_with(
            project_name="test_project",
            reference="file_ref_xyz",
        )
        self.assertEqual(returned_path, output_path)
        self.assertTrue(os.path.exists(output_path))

        with open(output_path, "r") as f:
            read_content = f.read()
        self.assertEqual(read_content, test_content)

        os.remove(output_path)