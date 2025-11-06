# apache-airflow-course
Course on LinkedIn learning https://www.linkedin.com/learning/learning-apache-airflow/

The Correct Credentials
Username: admin
Password: MafcbzeQGquhSXN3


# Postgres access
# password for loonycorn is password
psql -U loonycorn -d postgres -h localhost

# Delete entries in the tables:
psql -U loonycorn -d postgres -h localhost -c "
TRUNCATE TABLE customers CASCADE;
TRUNCATE TABLE customer_purchases CASCADE;
TRUNCATE TABLE complete_customer_details CASCADE;

DROP TABLE customers CASCADE;
DROP TABLE customer_purchases CASCADE;
DROP TABLE complete_customer_details CASCADE;
"


# Airflow config file location:
code ~/airflow/airflow.cfg

# Start postgres:
sudo service postgresql start
# Verify its running:
sudo service postgresql status

list of databases:
# Connect first
psql -U loonycorn -d postgres -h localhost

# Then inside psql:
\l

# Config: postgres metadata store in airflow.cfg file
sql_alchemy_conn = postgresql://loonycorn:password@localhost:5432/airflow_db
# Created the database first in posgresql with psql
CREATE DATABASE airflow_db
# also created user
CREATE USER loonycorn
# Local executor - runs on local machine and can run tasks in parallel with posgresql or mysql, not with sqlite
executor = LocalExecutor

# File connection
id: fs_default
type: File(path)
extra json: {"path":"/"}

# Create database
CREATE DATABASE laptop_db
# access database
\c laptop_db

# Existing database
postgres
# access psql
/c postgres
# connection
postgres_conn
# schema
postgres
# port
5432


# setup GCS connect:
Step 1: Move Service Account Key to Config Directory
# Create config directory (following course structure)
mkdir -p /workspaces/apache-airflow-course/config

# Move the key file to config directory
mv /workspaces/apache-airflow-course/medcostapi-9c1f46ea6e81.json \
   /workspaces/apache-airflow-course/config/gcp-service-account.json

# Secure the file
chmod 600 /workspaces/apache-airflow-course/config/gcp-service-account.json

# Add to .gitignore to prevent committing secrets
echo "config/gcp-service-account.json" >> /workspaces/apache-airflow-course/.gitignore
echo "config/*.json" >> /workspaces/apache-airflow-course/.gitignore

Step 2: Set Up Application Default Credentials (ADC)
# Set environment variable for Application Default Credentials
export GOOGLE_APPLICATION_CREDENTIALS="/workspaces/apache-airflow-course/config/gcp-service-account.json"

# Make it persistent - add to your shell profile
echo 'export GOOGLE_APPLICATION_CREDENTIALS="/workspaces/apache-airflow-course/config/gcp-service-account.json"' >> ~/.bashrc

# Verify it's set
echo $GOOGLE_APPLICATION_CREDENTIALS

# Test authentication
gcloud auth activate-service-account --key-file=$GOOGLE_APPLICATION_CREDENTIALS

# Verify access to your bucket
gsutil ls gs://mrfrepo/temp/


Step 3: Configure Airflow GCS Connection


# Verify access to your bucket
Now configure the Airflow connection. Two options:

Option A: Via Airflow UI (Recommended for Course)
Go to http://localhost:8080 (login with student:airflow123)
Navigate to Admin → Connections
Click + to add connection
Fill in:
Connection Id: gcs_connection
Connection Type: Google Cloud
Project Id: medcostapi
Keyfile Path: /workspaces/apache-airflow-course/config/gcp-service-account.json
Scopes: https://www.googleapis.com/auth/cloud-platform
Option B: Via CLI (Quick Setup)
# Add GCS connection via Airflow CLI
airflow connections add 'gcs_connection' \
    --conn-type 'google_cloud_platform' \
    --conn-extra '{
        "key_path": "/workspaces/apache-airflow-course/config/gcp-service-account.json",
        "project": "medcostapi",
        "scope": "https://www.googleapis.com/auth/cloud-platform"
    }'

# Verify the connection was added
airflow connections get gcs_connection

Step 4: Restart Airflow Services
# Restart with new environment variable
export GOOGLE_APPLICATION_CREDENTIALS="/workspaces/apache-airflow-course/config/gcp-service-account.json"


# Activities to test

dags/simple_branching_with_variable.py
dags/simple_sql_pipeline.py --> check do_xcom_push worked under display_result task


# Run RabbitMQ
# Start in detached mode again
sudo rabbitmq-server -detached

# Or start in foreground (for debugging)
sudo rabbitmq-server

# Set up VS Codespaces forwarding port
# Forward Port in VS Code
1. Open the PORTS panel in VS Code (View → Ports)
2. Click "Forward a Port"
3. Enter 15672
4. Set visibility to Public (for external access)
5. Click on the Local Address link shown in the Ports panel

# Access RabbitMQ Management UI
# Open your browser and go to:
http://137.0.0.1:15672
# Default credentials
Username: guest
Password: guest

# Create Admin User for Course
# Create admin user
sudo rabbitmqctl add_user admin airflow123

# Set admin tag
sudo rabbitmqctl set_user_tags admin administrator

# Grant full permissions
sudo rabbitmqctl set_permissions -p / admin ".*" ".*" ".*"

# Verify user created
sudo rabbitmqctl list_users


# Stop RabbitMQ gracefully
sudo rabbitmqctl stop

# Or stop the entire application
sudo rabbitmqctl stop_app

# Verify it stopped
sudo rabbitmqctl status

# Force kill by process name
sudo pkill -f rabbitmq-server

# Or find the PID and kill it
ps aux | grep rabbitmq-server
sudo kill <PID>

# Check if it's still running
sudo rabbitmqctl status
# Should show: "Status of node rabbit@codespaces-541d4f ...nodedown"


# Airflow Celery
airflow celery flower
# Access Airflow Celery Flower
# Open your browser and go to:
http://127.0.0.1:5555
# Default credentials
none required

# Create Celery Worker
# Correct command for Airflow 3.x
airflow celery worker

# Run in background
airflow celery worker &

# With specific queue
airflow celery worker --queues default,high_priority

# With concurrency settings
airflow celery worker --concurrency 4


Complete Airflow + Celery + RabbitMQ Setup
Step 1: Ensure PostgreSQL is Running
# Start PostgreSQL
sudo service postgresql start

# Verify
sudo service postgresql status

# Create Celery result backend database (if not exists)
sudo -u postgres psql << EOF
CREATE DATABASE airflow_metadata_db OWNER loonycorn;
GRANT ALL PRIVILEGES ON DATABASE airflow_metadata_db TO loonycorn;
EOF

Step 2: Configure Airflow for Celery Executor
# Set Celery configuration
export AIRFLOW__CELERY__BROKER_URL="amqp://admin:airflow123@localhost:5672//"
export AIRFLOW__CELERY__RESULT_BACKEND="db+postgresql://loonycorn:password@localhost:5432/airflow_metadata_db"
export AIRFLOW__CORE__EXECUTOR="CeleryExecutor"

# Make persistent
cat >> ~/.bashrc << 'EOF'
# Celery Executor Configuration
export AIRFLOW__CELERY__BROKER_URL="amqp://admin:airflow123@localhost:5672//"
export AIRFLOW__CELERY__RESULT_BACKEND="db+postgresql://loonycorn:password@localhost:5432/airflow_metadata_db"
export AIRFLOW__CORE__EXECUTOR="CeleryExecutor"
EOF

# Verify configuration
airflow config get-value core executor
airflow config get-value celery broker_url
airflow config get-value celery result_backend

Step 3: Start All Required Services
Open 3 separate terminals (or use background processes):

Terminal 1: RabbitMQ
# Start RabbitMQ
sudo rabbitmq-server -detached

# Verify
sudo rabbitmqctl status

Terminal 2: Airflow Scheduler
# Start scheduler
airflow scheduler

Terminal 3: Celery Worker (Airflow 3.x method)
# Start Celery worker
airflow celery worker

# Or in background
airflow celery worker &

Step 4: Verify Celery Worker is Running
# List active Celery workers
airflow celery list

# Should show output like:
# * celery@codespaces-541d4f: OK

# Check worker status in detail
airflow celery inspect active

# Monitor with Flower (optional)
airflow celery flower
# Access at: http://localhost:5555



## Celery Worker Setup (Airflow 3.x)

### Configure Celery Executor
```bash
# Set environment variables
export AIRFLOW__CELERY__BROKER_URL="amqp://loonycorn:password@localhost:5672/"
export AIRFLOW__CELERY__RESULT_BACKEND="db+postgresql://loonycorn:password@localhost:5432/airflow_metadata_db"
export AIRFLOW__CORE__EXECUTOR="CeleryExecutor"

# Make persistent
cat >> ~/.bashrc << 'EOF'
export AIRFLOW__CELERY__BROKER_URL="amqp://loonycorn:password@localhost:5672/"
export AIRFLOW__CELERY__RESULT_BACKEND="db+postgresql://loonycorn:password@localhost:5432/airflow_metadata_db"
export AIRFLOW__CORE__EXECUTOR="CeleryExecutor"
EOF
```

### Create Celery Worker (Correct Method)
```bash
# ✅ Airflow 3.x way - use Airflow CLI
airflow celery worker

# Run in background
airflow celery worker &

# With specific concurrency
airflow celery worker --concurrency 4

# ❌ OLD WAY (doesn't work in Airflow 3.x):
# celery -A airflow.executors.celery_executor worker
```

### Verify Worker Registration
```bash
# List active workers
airflow celery list

# Check worker details
airflow celery inspect active

# Monitor with Flower UI
airflow celery flower
# Access: http://localhost:5555
```

### Complete Startup Sequence
```bash
# 1. Start RabbitMQ
sudo rabbitmq-server -detached

# 2. Start PostgreSQL
sudo service postgresql start

# 3. Start Airflow Scheduler (Terminal 1)
airflow scheduler

# 4. Start Celery Worker (Terminal 2) - REQUIRED for CeleryExecutor
airflow celery worker

# 5. Start Airflow Webserver (Terminal 3)
# airflow webserver
airflow standalone
```

### Monitor Distributed Execution
```bash
# Trigger DAG
airflow dags trigger car_data_processing_pipeline_01

# Watch RabbitMQ queues
watch -n 1 'sudo rabbitmqctl list_queues name messages consumers'

# Monitor Flower UI
"$BROWSER" http://localhost:5555

# Check worker logs
tail -f ~/airflow/logs/celery-*.log
```

Test Your Setup with Car Processing Pipeline
# Trigger the DAG
airflow dags trigger car_data_processing_pipeline_01

# Monitor task execution
airflow dags state car_data_processing_pipeline_01

# Watch RabbitMQ queues
watch -n 1 'sudo rabbitmqctl list_queues name messages consumers'

