from __future__ import annotations

import json
from typing import Any

import requests

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class FlightPathServerHook(BaseHook):
    """
    Hook for FlightPath Server API.

    :param flightpath_server_conn_id: The Airflow connection ID for FlightPath Server.
    """

    conn_name_attr = "flightpath_server_conn_id"
    default_conn_name = "flightpath_server_default"
    conn_type = "flightpath_server"
    hook_name = "FlightPath Server"

    def __init__(self, flightpath_server_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.flightpath_server_conn_id = flightpath_server_conn_id
        self.conn = self.get_connection(flightpath_server_conn_id)
        self.base_url = self.conn.host
        self.api_key = self.conn.password  # Assuming API key is stored in password field

        if not self.base_url:
            raise AirflowException("FlightPath Server base URL not found in connection.")
        if not self.api_key:
            raise AirflowException("FlightPath Server API key not found in connection (password field).")

        self.headers = {
            "access_token": self.api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _call_api(self, endpoint: str, method: str = "POST", data: dict | None = None) -> dict:
        """
        Makes an API call to the FlightPath Server.

        :param endpoint: The API endpoint to call (e.g., "/csvpath/register_file").
        :param method: The HTTP method to use (e.g., "POST", "GET").
        :param data: The request body as a dictionary.
        :return: The JSON response from the API.
        :raises AirflowException: If the API call fails or returns an error.
        """
        url = f"{self.base_url}{endpoint}"
        self.log.info("Calling FlightPath Server API: %s %s", method, url)

        try:
            if method == "POST":
                response = requests.post(url, headers=self.headers, data=json.dumps(data))
            else:
                raise AirflowException(f"Unsupported HTTP method: {method}")

            response.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)
            return response.json()
        except requests.exceptions.RequestException as e:
            raise AirflowException(f"FlightPath Server API call failed: {e}")
        except json.JSONDecodeError as e:
            raise AirflowException(f"Failed to decode JSON response from FlightPath Server: {e}, Response: {response.text}")

    def register_file(
        self,
        project_name: str,
        name: str,
        file_location: str,
        template: str | None = None,
    ) -> dict:
        """
        Registers a new version of a named file using /csvpath/register_file.

        :param project_name: The name of the project.
        :param name: The name of the file to register.
        :param file_location: The location of the file.
        :param template: Optional. The template to use.
        :return: The JSON response containing the reference.
        """
        data = {
            "project_name": project_name,
            "name": name,
            "file_location": file_location,
        }
        if template:
            data["template"] = template
        return self._call_api("/csvpath/register_file", method="POST", data=data)

    def register_and_run(
        self,
        project_name: str,
        file_location: str,
        file_name: str,
        csvpaths_group_name: str,
        method: str = "collect_paths",
        file_template: str | None = None,
        run_template: str | None = None,
    ) -> dict:
        """
        Registers a new version of a named-file and then runs it through a named-paths group
        using /csvpath/register_and_run.

        :param project_name: The name of the project.
        :param file_location: The location of the file.
        :param file_name: The name of the file.
        :param csvpaths_group_name: The name of the csvpaths group.
        :param method: Optional. The run method (e.g., "collect_paths"). Defaults to "collect_paths".
        :param file_template: Optional. The file template.
        :param run_template: Optional. The run template.
        :return: The JSON response containing references.
        """
        data = {
            "project_name": project_name,
            "file_location": file_location,
            "file_name": file_name,
            "csvpaths_group_name": csvpaths_group_name,
            "method": method,
        }
        if file_template:
            data["file_template"] = file_template
        if run_template:
            data["run_template"] = run_template
        return self._call_api("/csvpath/register_and_run", method="POST", data=data)

    def find_files(self, project_name: str, reference: str) -> dict:
        """
        Finds the paths of registered named-files based on a reference using /find/find_files.

        :param project_name: The name of the project.
        :param reference: The reference to find files.
        :return: The JSON response containing a list of file paths.
        """
        data = {
            "project_name": project_name,
            "reference": reference,
        }
        return self._call_api("/find/find_files", method="POST", data=data)

    def get_file(self, project_name: str, reference: str) -> dict:
        """
        Returns the base64 encoded content of a file found by a named-file reference using /find/get_file.

        :param project_name: The name of the project.
        :param reference: The reference to the file.
        :return: The JSON response containing the file content.
        """
        data = {
            "project_name": project_name,
            "reference": reference,
        }
        return self._call_api("/find/get_file", method="POST", data=data)
