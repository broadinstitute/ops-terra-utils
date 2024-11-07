GCP = "gcp"
AZURE = "azure"
ARG_DEFAULTS = {
    "max_retries": 5,
    "max_backoff_time": 5 * 60,
    "update_strategy": "REPLACE",
    "multithread_workers": 10,
    "batch_size": 500,
    "batch_size_to_list_files": 20000,
    "batch_size_to_delete_files": 200,
    "file_ingest_batch_size": 500,
    "waiting_time_to_poll": 90,
    "docker_image": "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"
}


def comma_separated_list(value: str) -> list:
    """Return a list of values from a comma-separated string. Can be used as type in argparse."""
    return value.split(",")
