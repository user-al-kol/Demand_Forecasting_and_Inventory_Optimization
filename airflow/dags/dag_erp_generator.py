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
    "retries":          0,
    "email_on_failure": False,
}

# =============================================================================
# DAG definition
# =============================================================================

with DAG(
    dag_id="erp_csv_generator_ingestion",
    description="Simulates ERP end-of-day CSV export every 3 minutes",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule=None,      # "*/3 * * * *" every 3 real minutes = 1 simulated day
    catchup=False,               # don't backfill missed runs
    max_active_runs=1,           # never run two generator instances at once
    tags=["simulation", "ingestion", "erp"],
) as dag:

    generate_erp_dump = DockerOperator(
        task_id="generate_erp_dump",
        image="belsani-erp-generator:latest",   # built from ./erp_generator/Dockerfile
        container_name="belsani_erp_generator",
        auto_remove="success",                  # turn to "success" after testing, clean up container after success
        docker_url="unix://var/run/docker.sock", # You'll need to add this to the 
                                                 # airflow-worker service in your docker-compose.yaml: 
                                                 # - /var/run/docker.sock:/var/run/docker.sock
        network_mode="demand_forecasting_optimisation_inventory_default", # same network as postgres_oltp
        mounts=[
            Mount(
                source="/home/alex/demand_forecasting_optimisation_inventory/data/erp_dumps",
                target="/app/erp_dumps",
                type="bind",
            )
        ],
        environment={
            "OLTP_HOST":     "postgres_oltp",
            "OLTP_PORT":     "5432",
            "OLTP_DB":       "belsani_oltp",
            "OLTP_USER":     "belsani",
            "OLTP_PASSWORD": "belsani_secret",
            "OUTPUT_DIR":    "/app/erp_dumps",
            "MIN_ORDERS":    "5",
            "MAX_ORDERS":    "15",
            "MIN_MOVEMENTS": "20",
            "MAX_MOVEMENTS": "50",
        },
    )
    ingestion = DockerOperator(
        task_id="ingestion",
        image="belsani_ingestion:latest",
        container_name="belsani_ingestor",
        auto_remove="success", # turn to "success" after testing
        docker_url="unix://var/run/docker.sock",
        network_mode="demand_forecasting_optimisation_inventory_default",
        mounts=[
            Mount(
                source="/home/alex/demand_forecasting_optimisation_inventory/data/erp_dumps",
                target="/app/erp_dumps",
                type="bind",
            ),
            Mount(
                source="/home/alex/demand_forecasting_optimisation_inventory/data/delta_lake/bronze/erp_inventory_movements_raw",
                target="/app/erp_inventory_movements_raw",
                type="bind",
            ),
            Mount(
                source="/home/alex/demand_forecasting_optimisation_inventory/data/delta_lake/bronze/erp_sales_raw",
                target="/app/erp_sales_raw",
                type="bind",
            ),
        ],
        environment={
            "SOURCE_DIR": "/app/erp_dumps",
            "IM_SOURCE_DIR": "/app/erp_inventory_movements_raw",
            "S_SOURCE_DIR": "/app/erp_sales_raw",
            "LOGICAL_DATE": "{{ logical_date.isoformat() }}",
            "IM_DESTINATION_DIR": "/app/erp_inventory_movements_raw",
            "S_DESTINATION_DIR": "/app/erp_sales_raw"
        }
    )
    bronze_upsert = DockerOperator(
        task_id="bronze_upsert",
        image="bronze_upsert:latest",
        container_name="bronze-upsert-container",
        auto_remove="never", # turn to "success" after testing
        docker_url="unix://var/run/docker.sock",
        network_mode="demand_forecasting_optimisation_inventory_default",
        mounts=[
            Mount(
                source="/home/alex/demand_forecasting_optimisation_inventory/data/delta_lake/bronze/erp_inventory_movements_raw",
                target="/app/erp_inventory_movements_raw",
                type="bind",
            ),
            Mount(
                source="/home/alex/demand_forecasting_optimisation_inventory/data/delta_lake/bronze/erp_sales_raw",
                target="/app/erp_sales_raw",
                type="bind",
            )
        ],
        environment={
            "IM_SOURCE_DIR": "/app/erp_inventory_movements_raw",
            "S_SOURCE_DIR": "/app/erp_sales_raw",
            "LOGICAL_DATE": "{{ logical_date.isoformat() }}"
        }
    )


    generate_erp_dump >> ingestion >> bronze_upsert
    
