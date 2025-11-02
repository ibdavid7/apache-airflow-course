import pandas as pd
from datetime import datetime, timedelta


from airflow.models import Variable
from airflow.sdk import DAG, dag, task
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator


default_args = {
    'owner' : 'loonycorn'
}

START_DATE = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=5)



@dag(
    dag_id = 'branching_using_operators_03',
    description = 'Branching using operators',
    default_args = default_args,
    start_date = START_DATE,
    schedule = '@once',
    tags = ['branching', 'python', 'operators']
)
def branching_using_operators():

    @task(task_id='read_csv_file_task')
    def read_csv_file():
        df = pd.read_csv('/workspaces/apache-airflow-course/datasets/car_data.csv')

        print(df)

        return df.to_json()

    @task(task_id='filter_two_seaters_task')
    def filter_two_seaters(ti, **kwargs):
        
        print(f'**kwargs:${kwargs}')
        
        json_data = ti.xcom_pull(task_ids='read_csv_file_task')
        
        df = pd.read_json(json_data)
        
        two_seater_df = df[df['Seats'] == 2]
        
        # print df
        # print(two_seater_df)
        
        ti.xcom_push(key='transform_result', value=two_seater_df.to_json())
        ti.xcom_push(key='transform_filename', value='two_seaters')

    @task(task_id='filter_fwds_task')
    def filter_fwds(ti, **kwargs):
        
        print(f'**kwargs:${kwargs}')
        
        json_data = ti.xcom_pull(task_ids='read_csv_file_task')
        
        df = pd.read_json(json_data)
        
        fwd_df = df[df['PowerTrain'] == 'FWD']
        
        ti.xcom_push(key='transform_result', value=fwd_df.to_json())
        ti.xcom_push(key='transform_filename', value='fwds')

    @task(task_id='write_csv_result_task', trigger_rule='none_failed')
    def write_csv_result(ti, **kwargs): 
        
        print(f'**kwargs:${kwargs}')

        results = ti.xcom_pull(
            task_ids=['filter_two_seaters_task', 'filter_fwds_task'],
            key='transform_result',
        )
        filenames = ti.xcom_pull(
            task_ids=['filter_two_seaters_task', 'filter_fwds_task'],
            key='transform_filename',
        )

        json_data = next((r for r in (results or []) if r), None)
        file_name = next((f for f in (filenames or []) if f), None)

        if not json_data or not file_name:
            raise ValueError("No transform output available to write.")

        df = pd.read_json(json_data)
        print(f'Resulting DataFrame for {file_name}:')
        print(df)

        output_path = f'/workspaces/apache-airflow-course/output/{file_name}.csv'
        print(f'Writing output to {output_path}')
        df.to_csv(output_path, index=False)
    
    @task.branch(task_id='determine_branch_task')
    def determine_branch():
        final_output = Variable.get("transform", default_var=None)

        if final_output == 'filter_two_seaters':
            return 'filter_two_seaters_task'
        elif final_output == 'filter_fwds':
            return 'filter_fwds_task'
  
    read_csv_file_task = read_csv_file()
    determine_branch_task = determine_branch()
    filter_two_seaters_task = filter_two_seaters()
    filter_fwds_task = filter_fwds()
    write_csv_result_task = write_csv_result()
    
    read_csv_file_task >> determine_branch_task >> [filter_two_seaters_task, filter_fwds_task] >> write_csv_result_task
        
    


dag = branching_using_operators()


