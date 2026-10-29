__version__ = "0.1.0"

## This is needed to allow Airflow to pick up specific metadata fields it needs for certain features.
def get_provider_info():
    return {
        "package-name": "flightpath-server-airflow-provider",  # Required
        "name": "FlightPath Server",  # Required
        "description": "An Apache Airflow provider for FlightPath Server.",  # Required
        "connection-types": [
            {
                "connection-type": "flightpath_server",
                "hook-class-name": "flightpath_server_provider.hooks.flightpath_server.FlightPathServerHook"
            }
        ],
        "versions": [__version__],  # Required
    }
