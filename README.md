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

# Activities to test



dags/pipeline_with_postgres_hook.py
dags/pipeline_with_s3_hook.py


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
