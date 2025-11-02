"""
Course Example 01: Hello World DAG
Simple DAG to demonstrate basic Airflow concepts
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Default arguments for the DAG
default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
dag = DAG(
    'course_01_hello_world',
    default_args=default_args,
    description='Course example: Basic Hello World DAG',
    schedule=timedelta(days=1),  # Changed from schedule_interval to schedule
    catchup=False,
    tags=['course', 'basic', 'example']
)

def print_hello():
    """Simple function to print hello"""
    print("Hello from Airflow!")
    print("This is your first DAG running successfully!")
    return "Hello World task completed"

def print_date():
    """Function to print current date"""
    from datetime import datetime
    current_time = datetime.now()
    print(f"Current date and time: {current_time}")
    return f"Date task completed at {current_time}"

# Define tasks
hello_task = PythonOperator(
    task_id='hello_world_task',
    python_callable=print_hello,
    dag=dag
)

date_task = PythonOperator(
    task_id='print_date_task', 
    python_callable=print_date,
    dag=dag
)

# Set task dependencies
hello_task >> date_task