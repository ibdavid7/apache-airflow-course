import time
from datetime import datetime, timedelta


from airflow import DAG
from airflow.sdk import dag, task


default_args = {
    'owner' : 'loonycorn'
}

START_TIME = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=5)

@dag(
    dag_id = 'dag_with_operators_02',
    description = 'Python operators in DAGs',
    default_args = default_args,
    start_date = START_TIME,
    schedule = '@once',
    tags = ['dependencies', 'python', 'operators']
)

def dag_with_tasksflow_api():
    
    @task
    def task_a():
        print("TASK A executed!")
    
    @task
    def task_b():
        time.sleep(5)
        print("TASK B executed!")

    @task
    def task_c():
        time.sleep(5)
        print("TASK C executed!")

    @task
    def task_d():
        time.sleep(5)
        print("TASK D executed!")  

    @task
    def task_e():
        print("TASK E executed!")          

    task_a() >> [task_b(), task_c(), task_d()] >> task_e()

dag = dag_with_tasksflow_api()