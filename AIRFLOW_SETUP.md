# Airflow 3.1.0 Setup Guide

## ✅ Installation Complete!

Your Apache Airflow 3.1.0 environment has been successfully set up with example DAGs from the installation package.

## 🚀 Quick Start

### Start Airflow Services
```bash
./start-airflow.sh
```

### Access the Web UI
- **URL**: http://localhost:8080
- **Username**: `admin`
- **Password**: `admin`

## 📁 What's Installed

### DAGs Folder
- **Location**: `/workspaces/apache-airflow-course/dags`
- **Example DAGs**: 14 files copied from Airflow installation

### Example DAGs Included
1. `course_01_hello_world.py` - Custom course DAG
2. `tutorial*.py` - Official Airflow tutorials (7 files)
3. `example_simplest_dag.py` - Simplest possible DAG
4. `example_branch*.py` - Branching examples
5. `example_xcom*.py` - XCom (cross-communication) examples
6. `example_task_group*.py` - Task grouping examples

## 🔧 Configuration

### Key Settings
- **AIRFLOW_HOME**: `~/airflow`
- **DAGs Folder**: `/workspaces/apache-airflow-course/dags`
- **Database**: SQLite at `~/airflow/airflow.db`
- **Load Built-in Examples**: `False` (we copied them manually)

### Configuration File
Main config: `~/airflow/airflow.cfg`

## 📊 Verifying Installation

### Check Running Services
```bash
ps aux | grep airflow
```

### List All DAGs
```bash
airflow dags list
```

### Check for Import Errors
```bash
airflow dags list-import-errors
```

## 🛠️ Common Commands

### Manual Service Control
```bash
# Start Scheduler
airflow scheduler &

# Start API Server (Web UI)
airflow api-server &

# Stop All
pkill -f airflow
```

### DAG Operations
```bash
# List all DAGs
airflow dags list

# Test a specific DAG
airflow dags test <dag_id> <execution_date>

# Trigger a DAG run
airflow dags trigger <dag_id>
```

### Database Operations
```bash
# Initialize/upgrade database
airflow db migrate

# Reset database (⚠️  deletes all data)
airflow db reset
```

## 📝 Logs

- **Scheduler**: `~/airflow/scheduler.log`
- **API Server**: `~/airflow/apiserver.log`
- **Task Logs**: `~/airflow/logs/`

## 🆕 Airflow 3.x Breaking Changes

### What Changed from 2.x:
1. **`airflow webserver`** → **`airflow api-server`**
2. **`schedule_interval`** → **`schedule`** in DAG definition
3. **Operator imports**: Use `airflow.providers.standard.operators.*`
4. **No `airflow users` command**: Use Simple Auth Manager config
5. **Example DAGs**: Don't auto-populate custom folders

## 🎓 Learning Path

Start with these DAGs in order:
1. `example_simplest_dag.py` - Understand basic structure
2. `tutorial_dag.py` - Learn core concepts
3. `tutorial_taskflow_api.py` - Modern TaskFlow API
4. `example_xcom.py` - Task communication
5. `example_branch_labels.py` - Conditional logic
6. `example_task_group.py` - Organizing complex DAGs

## 🐛 Troubleshooting

### DAGs Not Appearing
- Wait 30-60 seconds after starting scheduler
- Check: `airflow dags list-import-errors`
- Verify scheduler is running: `ps aux | grep "airflow scheduler"`
- Check logs: `tail -f ~/airflow/scheduler.log`

### Can't Login
- Username: `admin`
- Password: `admin`
- Check config: `airflow config get-value auth_manager simple_auth_manager_users`

### Port Already in Use
- Kill existing processes: `pkill -f airflow`
- Check ports: `lsof -i :8080` and `lsof -i :8793`

## 📚 Additional Resources

- [Airflow Documentation](https://airflow.apache.org/docs/apache-airflow/3.1.0/)
- [DAG Writing Best Practices](https://airflow.apache.org/docs/apache-airflow/3.1.0/best-practices.html)
- [TaskFlow API Tutorial](https://airflow.apache.org/docs/apache-airflow/3.1.0/tutorial/taskflow.html)

---

**Ready to learn Apache Airflow!** 🎉
