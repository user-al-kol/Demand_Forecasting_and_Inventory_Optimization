# On the VM, logged in as root or your initial sudo user:
bash 01_create_user.sh

# Log out completely
exit

# SSH back in as alex
ssh alex@<your-vm-ip>

# Run the Docker installer
bash 02_install_docker.sh

# Log out again (required for docker group)
exit

# SSH back in as alex one final time
ssh alex@<your-vm-ip>

# Verify — should run without sudo
docker run hello-world
docker compose version

# After reconnecting and uploading your project folder
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.1.8/docker-compose.yaml'
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" >> .env      # append, don't overwrite
# --- edit docker-compose.yaml: add postgres_oltp service ---

docker compose up airflow-init              # wait for code 0
docker compose up -d                        # start everything
docker compose ps

# Check oltp postgres
# Connect to the DB
docker exec -it belsani_postgres psql -U belsani -d belsani_oltp

# Once inside psql:
\dt                              -- list all tables
SELECT COUNT(*) FROM products;   -- should be 50
SELECT COUNT(*) FROM sales_orders; -- should be 121
SELECT * FROM v_inventory_status LIMIT 10;
SELECT * FROM v_supplier_performance;