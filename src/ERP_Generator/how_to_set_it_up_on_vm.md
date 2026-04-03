# 1. Create the shared output directory
mkdir -p ~/belsani-pipeline/erp_dumps

# 2. Place the erp_generator/ folder in your project
# ~/belsani-pipeline/erp_generator/

# 3. Build the Docker image (run from inside erp_generator/)
cd ~/belsani-pipeline/erp_generator
docker build -t belsani-erp-generator:latest .

# 4. Copy the DAG to Airflow's dags folder
cp dag_erp_generator.py ~/belsani-pipeline/dags/

# 5. Test it manually before letting Airflow run it
docker run --rm \
  --network demand_forecasting_optimasation-inventory_default \
  -v ~/demand_forecasting_optimasation-inventory/erp_dumps:/erp_dumps \
  -e OLTP_HOST=postgres_oltp \
  -e OLTP_USER=belsani \
  -e OLTP_PASSWORD=belsani_secret \
  -e OLTP_DB=belsani_oltp \
  belsani-erp-generator:latest

Three design decisions worth understanding

Simulated date is derived from file count, not the real clock. The generator counts how many sales_*.csv files already exist in erp_dumps/ and advances the calendar by that many working days from 2025-01-01. This means it's deterministic — run 1 is always Monday January 6th, run 2 is Tuesday January 7th, and so on. Weekends are skipped automatically.

The DockerOperator needs access to the Docker socket. The docker_url="unix://var/run/docker.sock" line means Airflow's worker container must have the host's Docker socket mounted. You'll need to add this to the airflow-worker service in your docker-compose.yaml: - /var/run/docker.sock:/var/run/docker.sock. Without it the DAG fails immediately.

network_mode must match your actual Docker network name. The generator container needs to reach postgres_oltp by service name. Run docker network ls on the VM to confirm your network name — it's typically demand_forecasting_optimasation-inventory_default but may differ depending on your project directory name.