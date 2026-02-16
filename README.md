<p align="center">
  <a href="https://www.airflow.apache.org">
    <img alt="Airflow" src="https://cwiki.apache.org/confluence/download/attachments/145723561/airflow_transparent.png?api=v2" width="60" />
  </a>
</p>
<h1 align="center">
  Airflow FlightPath Server Provider
</h1>
  <h3 align="center">
  An Apache Airflow provider for FlightPath Server, enabling seamless integration with FlightPath Data projects.
</h3>

<br/>

This repository provides an Apache Airflow provider for the FlightPath Server API, allowing you to orchestrate data preboarding tasks directly from your Airflow DAGs. It enables:
- Registering new data files (CSV, Excel, JSONL).
- Registering files and triggering FlightPath pipelines.
- Pulling processed data from past runs.

## Formatting Standards

The provider adheres to the following file structure:

```bash
├── LICENSE
├── README.md
├── flightpath_server_provider
│   ├── __init__.py
│   ├── example_dags
│   ├── hooks
│   │   ├── __init__.py
│   │   └── flightpath_server.py
│   └── operators
│       ├── __init__.py
│       └── flightpath_server.py
├── pyproject.toml
└── tests
    ├── __init__.py
    ├── hooks
    │   ├── __init__.py
    │   └── test_flightpath_server_hook.py
    └── operators
        ├── __init__.py
        └── test_flightpath_server_operator.py
```

## Installation

To install the FlightPath Server Provider, you can build it from source:

1.  Clone this repository:
    ```bash
    git clone https://github.com/dk107dk/airflow-provider-flightpath.git
    cd airflow-provider-flightpath
    ```
2.  Build the wheel:
    ```bash
    python3 -m pip install build
    python3 -m build
    ```
3.  Install the wheel into your Airflow environment (replace `X.Y.Z` with the actual version number from the `dist/` directory):
    ```bash
    pip install dist/airflow_provider_flightpath_server-X.Y.Z-py3-none-any.whl
    ```

### Airflow Connection Setup

Before using the operators, you need to configure an Airflow Connection for FlightPath Server:

1.  In the Airflow UI, go to `Admin -> Connections`.
2.  Click `+` to add a new connection.
3.  Set `Conn Id` to `flightpath_server_default` (or your preferred ID).
4.  Set `Conn Type` to `FlightPath Server`.
5.  Set `Host` to the URL of your FlightPath Server (e.g., `http://localhost:8000`).
6.  Set `Password` to your FlightPath Server API key.

## Usage Examples

Here are examples of how to use the FlightPath Server operators in your DAGs.

### `FlightPathServerRegisterFileOperator`

This operator registers a new CSV, Excel, or JSONL file with the FlightPath Server.

```python
from airflow import DAG
from datetime import datetime
from airflow.providers.flightpath_server.operators.flightpath_server import FlightPathServerRegisterFileOperator

with DAG(
    dag_id='flightpath_register_file_example',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['flightpath', 'example'],
) as dag:
    register_new_file = FlightPathServerRegisterFileOperator(
        task_id='register_data_file',
        project_name='my_data_project',
        name='sales_data.csv',
        file_location='/path/to/my/sales_data.csv',
        # template='my_file_template', # Optional: if you have a specific template
        flightpath_server_conn_id='flightpath_server_default',
    )
```

### `FlightPathServerRegisterAndRunOperator`

This operator registers a new file and immediately triggers a FlightPath pipeline (named-paths group) to process it.

```python
from airflow import DAG
from datetime import datetime
from airflow.providers.flightpath_server.operators.flightpath_server import FlightPathServerRegisterAndRunOperator

with DAG(
    dag_id='flightpath_register_and_run_example',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['flightpath', 'example'],
) as dag:
    register_and_run_pipeline = FlightPathServerRegisterAndRunOperator(
        task_id='register_and_run_pipeline',
        project_name='my_data_project',
        file_location='/path/to/incoming/orders.jsonl',
        file_name='orders_Q4.jsonl',
        csvpaths_group_name='process_orders_pipeline',
        # method='fast_forward_paths', # Optional: default is 'collect_paths'
        # file_template='orders_template', # Optional
        # run_template='daily_run_template', # Optional
        flightpath_server_conn_id='flightpath_server_default',
    )
```

### `FlightPathServerPullDataOperator`

This operator retrieves the content of a file from a past FlightPath run and saves it to a local path.

```python
from airflow import DAG
from datetime import datetime
from airflow.providers.flightpath_server.operators.flightpath_server import FlightPathServerPullDataOperator

with DAG(
    dag_id='flightpath_pull_data_example',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['flightpath', 'example'],
) as dag:
    pull_processed_report = FlightPathServerPullDataOperator(
        task_id='get_processed_report',
        project_name='my_data_project',
        # The reference could be obtained from the output of a previous RegisterAndRunOperator task
        # e.g., 'XCom.pull(task_ids="register_and_run_pipeline", key="return_value")["run_reference"]'
        reference='my_report_2023-10-26T10:00:00.csv', # Example reference
        output_path='/tmp/processed_report_2023-10-26.csv',
        flightpath_server_conn_id='flightpath_server_default',
    )
```

## Development Standards

All modules must follow a specific set of best practices to optimize their performance with Airflow:

- **All classes should always be able to run without access to the internet.** The Airflow Scheduler parses DAGs on a regular schedule. Every time that parse happens, Airflow will execute whatever is contained in the `init` method of your class. If that `init` method contains network requests, such as calls to a third party API, there will be problems due to repeated network calls.
- **Init methods should never call functions which return valid objects only at runtime**. This will cause a fatal import error when trying to import a module into a DAG. A common best practice for referencing connectors and variables within DAGs is to use [Jinja Templating](https://airflow.apache.org/docs/apache-airflow/stable/concepts.html#jinja-templating).
- **All operator modules need an `execute` method.** This method defines the logic that the operator will implement.

## Unit testing

Your top-level `tests/` folder should include unit tests for all modules that exist in the repository. You can write tests in the framework of your choice, but the Astronomer team and Airflow community typically use [pytest](https://docs.pytest.org/en/stable/).

You can test this package by running: `python3 -m unittest` from the top-level of the directory.
