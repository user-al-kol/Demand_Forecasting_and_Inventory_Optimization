"""
dag_erp_generator.py
====================
Triggers the ERP CSV generator container every 3 minutes (= 1 simulated day).

Uses DockerOperator to run the generator as an isolated container,
passing environment variables for the OLTP DB connection and output path.

The generated CSVs land in the shared ./erp_dumps volume which is also
mounted in the downstream ingestion DAG.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# =============================================================================
# Default args
# =============================================================================

default_args = {
    "owner":            "belsani",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=1),
    "email_on_failure": False,
}

# =============================================================================
# DAG definition
# =============================================================================

with DAG(
    dag_id="erp_csv_generator",
    description="Simulates ERP end-of-day CSV export every 3 minutes",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="*/3 * * * *",      # every 3 real minutes = 1 simulated day
    catchup=False,               # don't backfill missed runs
    max_active_runs=1,           # never run two generator instances at once
    tags=["simulation", "ingestion", "erp"],
) as dag:

    generate_erp_dump = DockerOperator(
        task_id="generate_erp_dump",
        image="belsani-erp-generator:latest",   # built from ./erp_generator/Dockerfile
        container_name="erp_generator_run",
        auto_remove="success",                  # clean up container after success
        docker_url="unix://var/run/docker.sock", # You'll need to add this to the 
                                                 # airflow-worker service in your docker-compose.yaml: 
                                                 # - /var/run/docker.sock:/var/run/docker.sock
        network_mode="belsani-pipeline_default", # same network as postgres_oltp
        mounts=[
            Mount(
                source="/home/alex/belsani-pipeline/erp_dumps",
                target="/erp_dumps",
                type="bind",
            )
        ],
        environment={
            "OLTP_HOST":     "postgres_oltp",
            "OLTP_PORT":     "5432",
            "OLTP_DB":       "belsani_oltp",
            "OLTP_USER":     "belsani",
            "OLTP_PASSWORD": "belsani_secret",
            "OUTPUT_DIR":    "/erp_dumps",
            "MIN_ORDERS":    "5",
            "MAX_ORDERS":    "15",
            "MIN_MOVEMENTS": "20",
            "MAX_MOVEMENTS": "50",
        },
    )
