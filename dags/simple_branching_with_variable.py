from datetime import datetime, timedelta

from airflow import DAG

from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator


from airflow.models import Variable


default_args = {
   'owner' : 'loonycorn'
}

DEFAULT_START_DATE = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=5)

Variable.set("choice", "C")  # Possible values: "C" or "E"

def choose_branch():
    choice = Variable.get("choice", default_var=False)

    return choice

def branch(ti):
    choice = ti.xcom_pull(task_ids='taskChoose')

    if choice == 'C':
        return 'taskC'
    elif choice == 'E':
        return 'taskE'

def task_c():
    print("TASK C executed!")


with DAG(
    dag_id = 'simple_branching_with_variable',
    description = 'Simple branching pipeline using variables',
    default_args = default_args,
    start_date = DEFAULT_START_DATE,
    schedule = '@once',
    tags = ['pipeline', 'branching', 'variables']
) as dag:

    taskA = BashOperator(
        task_id = 'taskA',
        bash_command = 'echo TASK A has executed!'
    )

    taskChoose = PythonOperator(
        task_id = 'taskChoose',
        python_callable = choose_branch
    )

    taskBranch = BranchPythonOperator(
        task_id = 'taskBranch',
        python_callable = branch
    )

    taskC = PythonOperator(
        task_id = 'taskC',
        python_callable = task_c
    )

    taskD = BashOperator(
        task_id = 'taskD',
        bash_command = 'echo TASK D has executed!'
    )

    taskE = EmptyOperator(
        task_id = 'taskE',
    )


taskA >> taskChoose >> taskBranch >> [taskC, taskE]

taskC >> taskD