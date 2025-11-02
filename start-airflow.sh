#!/bin/bash
# Airflow 3.1.0 Startup Script for Apache Airflow Course

# Stop any existing Airflow processes
echo "Stopping existing Airflow processes..."
pkill -f "airflow scheduler"
pkill -f "airflow api-server"
sleep 2

# Start Scheduler
echo "Starting Airflow Scheduler..."
nohup airflow scheduler > ~/airflow/scheduler.log 2>&1 &
SCHEDULER_PID=$!
echo "Scheduler started with PID: $SCHEDULER_PID"

# Wait a moment for scheduler to initialize
sleep 3

# Start API Server (Web UI)
echo "Starting Airflow API Server (Web UI)..."
nohup airflow api-server > ~/airflow/apiserver.log 2>&1 &
APISERVER_PID=$!
echo "API Server started with PID: $APISERVER_PID"

echo ""
echo "============================================"
echo "Airflow 3.1.0 is starting up..."
echo "============================================"
echo "Web UI: http://localhost:8080"
echo "Username: admin"
echo "Password: admin"
echo ""
echo "DAGs Folder: /workspaces/apache-airflow-course/dags"
echo "Example DAGs copied: 14 files"
echo ""
echo "Wait 30-60 seconds for DAGs to appear in the UI"
echo ""
echo "To check logs:"
echo "  Scheduler: tail -f ~/airflow/scheduler.log"
echo "  API Server: tail -f ~/airflow/apiserver.log"
echo ""
echo "To list DAGs: airflow dags list"
echo "============================================"
