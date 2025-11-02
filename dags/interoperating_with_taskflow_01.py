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
    dag_id = 'interoperating_with_taskflow_01',
    description = 'Interoperating traditional tasks with taskflow',
    default_args = default_args,
    start_date = START_DATE,
    schedule = '@once',
    tags = ['interoperating', 'taskflow', 'python', 'operators']
)
def interoperate_with_taskflow():

    def read_csv_file():
        df = pd.read_csv('/workspaces/apache-airflow-course/datasets/car_data.csv')

        print(df)

        return df.to_json()
    
    @task
    def filter_teslas(json_data):
        df = pd.read_json(json_data)

        print(f'Original DataFrame: {df}')

        tesla_df = df[df['Brand'] == 'Tesla ']
        

        print(f'Filtered Teslas DataFrame: {tesla_df}')

        return tesla_df.to_json()

    def write_csv_result(filtered_teslas_json):

        df = pd.read_json(filtered_teslas_json)
        print(f'Resulting DataFrame for filtered_teslas: ')
        print(df)

        output_path = f'/workspaces/apache-airflow-course/output/filtered_teslas.csv'
        print(f'Writing output to {output_path}')
        df.to_csv(output_path, index=False)

    
    read_csv_file_task = PythonOperator(
        task_id='read_csv_file_task',
        python_callable=read_csv_file
    )
    
    filtered_teslas_json = filter_teslas(read_csv_file_task.output)
    
    write_csv_result_task = PythonOperator(
        task_id='write_csv_result_task',
        python_callable=write_csv_result,
        op_kwargs={'filtered_teslas_json': filtered_teslas_json}
    )
  

    # read_csv_file_task >> filter_teslas_json >> write_csv_result_task


dag = interoperate_with_taskflow()


