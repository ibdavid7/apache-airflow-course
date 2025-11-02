# Apache Airflow Course - AI Coding Instructions

This repository is part of a LinkedIn Learning course on Apache Airflow. When working in this codebase, follow these patterns and conventions.

## Project Context
- **Purpose**: Educational repository for learning Apache Airflow concepts
- **Target**: Students following the LinkedIn Learning course
- **Focus**: Practical DAG development, operators, and workflow orchestration

## Apache Airflow Development Patterns

### DAG Structure
- Place DAGs in `dags/` directory (standard Airflow convention)
- Use descriptive DAG IDs following pattern: `course_<lesson>_<topic>`
- Set appropriate `start_date`, `schedule_interval`, and `catchup=False` for learning examples
- Include docstrings explaining the learning objective

Example DAG structure:
```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'course_01_basic_dag',
    default_args=default_args,
    description='Course example: Basic DAG structure',
    schedule_interval=timedelta(days=1),
    catchup=False
)
```

### Common Development Workflows

#### Local Development Setup
- Use `docker-compose.yml` for local Airflow instance
- Include environment variables in `.env` file (never commit secrets)
- Mount `dags/`, `plugins/`, and `logs/` directories to container

#### Testing DAGs
- Use `airflow dags test <dag_id> <execution_date>` for testing
- Validate DAG syntax: `python <dag_file.py>`
- Check DAG import: `airflow dags list`

#### Course-Specific Conventions
- **Incremental Learning**: Each DAG should build on previous concepts
- **Clear Comments**: Explain Airflow concepts inline for educational value
- **Error Handling**: Include examples of task failure scenarios and retries
- **Operators**: Demonstrate different operator types (Bash, Python, SQL, etc.)

### Directory Structure (when course content is added)
```
├── dags/                   # Airflow DAGs
│   ├── 01_basics/         # Basic concepts
│   ├── 02_operators/      # Operator examples
│   ├── 03_sensors/        # Sensor patterns
│   └── 04_advanced/       # Advanced topics
├── plugins/               # Custom operators/hooks
├── docker-compose.yml     # Local development environment
├── requirements.txt       # Python dependencies
└── config/               # Airflow configuration files
```

### Key Dependencies
- **airflow**: Core orchestration framework
- **pandas**: Data manipulation (common in data pipelines)
- **requests**: HTTP operations
- **psycopg2**: PostgreSQL connectivity (if using Postgres backend)

### Educational Code Patterns
- Start with simple `DummyOperator` examples
- Progress to `BashOperator` and `PythonOperator`
- Demonstrate XCom for task communication
- Show branching with `BranchPythonOperator`
- Include sensor examples for external dependencies

### Airflow 3.x Specific Setup & Authentication

#### Database Initialization (Critical First Step)
- **ALWAYS run** `airflow db migrate` before starting webserver
- Missing tables (`dag`, `slot_pool`, etc.) cause 500 errors in UI/API
- Database location: `~/airflow/airflow.db` (SQLite default)

#### Authentication in Airflow 3.x
- **No `airflow users` command** - use Simple Auth Manager instead
- Users configured via: `simple_auth_manager_users = username:password`
- Default auto-generated password shown during first webserver start
- Check current users: `airflow config get-value auth_manager simple_auth_manager_users`
- API access requires Basic Auth with configured credentials

#### Essential Setup Commands
```bash
# 1. Initialize database (required first)
airflow db migrate

# 2. Start scheduler (in background or separate terminal)
airflow scheduler &

# 3. Start webserver (note the auto-generated password)
airflow webserver

# 4. Check authentication config
airflow config list | grep simple_auth_manager
```

### Airflow 3.x Breaking Changes
- `schedule_interval` → `schedule` in DAG definition
- `airflow.operators.python.PythonOperator` → `airflow.providers.standard.operators.python.PythonOperator`
- No `airflow users` command - use Simple Auth Manager configuration
- `airflow webserver` → `airflow api-server` (completely different command)

### Why DAGs Don't Appear
1. **Missing DAGs folder**: Create `dags/` directory in workspace or Airflow home
2. **Scheduler not running**: DAGs only appear when scheduler is active
3. **Import errors**: Check `airflow dags list-import-errors` for syntax issues
4. **Wrong DAGs path**: Set `AIRFLOW__CORE__DAGS_FOLDER` environment variable
5. **Example DAGs don't auto-load**: In Airflow 3.x, even with `load_examples=True`, built-in examples don't auto-populate custom DAG folders - copy them manually from `/usr/local/python/3.12.1/lib/python3.12/site-packages/airflow/example_dags/`

### Common Debugging Steps
1. **Database Issues**: Run `airflow db migrate` if seeing "no such table" errors
2. **Auth Issues**: Check auto-generated password in webserver startup output
3. Check Airflow UI for task logs
4. Validate DAG syntax before deployment
5. Use `airflow tasks test` for individual task testing
6. Check scheduler and worker logs for system issues

## Course Learning Objectives
When adding content, ensure each example clearly demonstrates:
- DAG definition and scheduling concepts
- Task dependencies and execution order
- Operator usage patterns
- Error handling and retries
- Monitoring and logging practices