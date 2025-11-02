from datetime import datetime, timedelta

from airflow import DAG
import pendulum

from airflow.operators.bash import BashOperator

DEFAULT_START = pendulum.datetime(2024, 1, 1, tz="UTC")

default_args = {
    'owner': 'loonycord',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='executing_multiple_tasks',  # Explicitly setting dag_id
    description='A DAG to execute multiple tasks',  # Explicitly setting description
    default_args=default_args,  # Explicitly setting default_args
    start_date=DEFAULT_START,  # Explicitly setting start_date
    schedule='@once',  # Updated to use 'schedule' instead of 'schedule_interval'
    tags=['example', 'hello_world']
) as dag:
    
    taskA = BashOperator(
        task_id='print_hello_task_A',
        bash_command='''
            echo TASK A has started!
          
            for i in {1..10}
            do
                echo "Iteration $i"
            done
            
            echo TASK A has completed!
        ''',
        dag=dag,
    )
    taskB = BashOperator(
        task_id='print_hello_task_B',
        bash_command='''
            echo TASK B has started!
            sleep 4
            echo TASK B has completed!
        ''',
        dag=dag,
    )
    taskC = BashOperator(
        task_id='print_hello_task_C',
        bash_command='''
            echo TASK C has started!
            sleep 15
            echo TASK C has completed!
        ''',
        dag=dag,
    )
    taskD = BashOperator(
        task_id='print_hello_task_D',
        bash_command='''
            echo TASK D has completed!
        ''',
        dag=dag,
    )

taskA >> [taskB, taskC] >> taskD


# taskA.set_downstream(taskB)
# taskA >> taskB
# taskA.set_upstream(taskB)
# taskA << taskB

