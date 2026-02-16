from __future__ import annotations

import base64
from typing import Any

from airflow.models.baseoperator import BaseOperator
from airflow.providers.flightpath_server.hooks.flightpath_server import FlightPathServerHook


class FlightPathServerRegisterFileOperator(BaseOperator):
    """
    Registers a new version of a named file in FlightPath Server.

    :param task_id: The task ID.
    :param project_name: The name of the project.
    :param name: The name of the file to register.
    :param file_location: The location of the file.
    :param template: Optional. The template to use.
    :param flightpath_server_conn_id: The Airflow connection ID for FlightPath Server.
    """

    template_fields = (
        "project_name",
        "name",
        "file_location",
        "template",
    )

    def __init__(
        self,
        *,
        project_name: str,
        name: str,
        file_location: str,
        template: str | None = None,
        flightpath_server_conn_id: str = FlightPathServerHook.default_conn_name,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_name = project_name
        self.name = name
        self.file_location = file_location
        self.template = template
        self.flightpath_server_conn_id = flightpath_server_conn_id

    def execute(self, context: Any) -> Any:
        hook = FlightPathServerHook(flightpath_server_conn_id=self.flightpath_server_conn_id)
        self.log.info(
            "Registering file '%s' in project '%s' from location '%s'",
            self.name,
            self.project_name,
            self.file_location,
        )
        response = hook.register_file(
            project_name=self.project_name,
            name=self.name,
            file_location=self.file_location,
            template=self.template,
        )
        self.log.info("File registered successfully. Reference: %s", response.get("reference"))
        return response


class FlightPathServerRegisterAndRunOperator(BaseOperator):
    """
    Registers a new version of a named-file and then runs it through a named-paths group.

    :param task_id: The task ID.
    :param project_name: The name of the project.
    :param file_location: The location of the file.
    :param file_name: The name of the file.
    :param csvpaths_group_name: The name of the csvpaths group.
    :param method: Optional. The run method (e.g., "collect_paths"). Defaults to "collect_paths".
    :param file_template: Optional. The file template.
    :param run_template: Optional. The run template.
    :param flightpath_server_conn_id: The Airflow connection ID for FlightPath Server.
    """

    template_fields = (
        "project_name",
        "file_location",
        "file_name",
        "csvpaths_group_name",
        "method",
        "file_template",
        "run_template",
    )

    def __init__(
        self,
        *,
        project_name: str,
        file_location: str,
        file_name: str,
        csvpaths_group_name: str,
        method: str = "collect_paths",
        file_template: str | None = None,
        run_template: str | None = None,
        flightpath_server_conn_id: str = FlightPathServerHook.default_conn_name,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_name = project_name
        self.file_location = file_location
        self.file_name = file_name
        self.csvpaths_group_name = csvpaths_group_name
        self.method = method
        self.file_template = file_template
        self.run_template = run_template
        self.flightpath_server_conn_id = flightpath_server_conn_id

    def execute(self, context: Any) -> Any:
        hook = FlightPathServerHook(flightpath_server_conn_id=self.flightpath_server_conn_id)
        self.log.info(
            "Registering and running file '%s' from location '%s' with csvpaths group '%s'",
            self.file_name,
            self.file_location,
            self.csvpaths_group_name,
        )
        response = hook.register_and_run(
            project_name=self.project_name,
            file_location=self.file_location,
            file_name=self.file_name,
            csvpaths_group_name=self.csvpaths_group_name,
            method=self.method,
            file_template=self.file_template,
            run_template=self.run_template,
        )
        self.log.info(
            "File registered and run successfully. Register Reference: %s, Run Reference: %s",
            response.get("register_reference"),
            response.get("run_reference"),
        )
        return response


class FlightPathServerPullDataOperator(BaseOperator):
    """
    Pulls data from a past run in FlightPath Server and saves it to a file.

    :param task_id: The task ID.
    :param project_name: The name of the project.
    :param reference: The reference to the file to retrieve.
    :param output_path: The local path where the retrieved file content will be saved.
    :param flightpath_server_conn_id: The Airflow connection ID for FlightPath Server.
    """

    template_fields = (
        "project_name",
        "reference",
        "output_path",
    )

    def __init__(
        self,
        *,
        project_name: str,
        reference: str,
        output_path: str,
        flightpath_server_conn_id: str = FlightPathServerHook.default_conn_name,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_name = project_name
        self.reference = reference
        self.output_path = output_path
        self.flightpath_server_conn_id = flightpath_server_conn_id

    def execute(self, context: Any) -> Any:
        hook = FlightPathServerHook(flightpath_server_conn_id=self.flightpath_server_conn_id)
        self.log.info(
            "Attempting to pull data for reference '%s' from project '%s'",
            self.reference,
            self.project_name,
        )
        response = hook.get_file(
            project_name=self.project_name,
            reference=self.reference,
        )
        file_content_base64 = response.get("file")
        if not file_content_base64:
            raise AirflowException("No file content received from FlightPath Server.")

        file_content = base64.b64decode(file_content_base64)

        with open(self.output_path, "wb") as f:
            f.write(file_content)
        self.log.info("File content successfully saved to %s", self.output_path)
        return self.output_path
