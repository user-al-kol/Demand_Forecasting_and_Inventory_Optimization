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