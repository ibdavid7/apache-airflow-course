from datetime import datetime, timedelta
import pandas as pd
from airflow.sdk import DAG, task
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


import psycopg2
import glob

START_DATE = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=5)

default_args = {
    'owner': 'loonycorn',
    'start_date': START_DATE,
    'schedule_interval': '@once',
}


FILE_PATH = '/workspaces/apache-airflow-course/tmp/laptops_*.csv'
FILE_COLS = ['Id', 'Company', 'Product', 'TypeName', 'Price_euros']
OUTPUT_FILE = '/workspaces/apache-airflow-course/output/{}.csv'



# Custom task to find and return the file
@task
def find_file():
    """Find the first matching file and return its path."""
    files = glob.glob(FILE_PATH)
    if not files:
        raise FileNotFoundError(f"No files matching {FILE_PATH}")
    
    found_file = files[0]  # Get first match
    print(f"Found {len(files)} file(s): {files}")
    return files  # Returns list via XCom

@task
def insert_laptop_data(filepaths: list):
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="loonycorn",
        password="password"
    )

    cur = conn.cursor()

    for filepath in filepaths:
        df = pd.read_csv(filepath, usecols=FILE_COLS)

        records = df.to_dict('records')
        
        for record in records:
            query = f"""INSERT INTO laptops 
                        (id, company, product, type_name, price_euros) 
                        VALUES (
                            {record['Id']}, 
                            '{record['Company']}', 
                            '{record['Product']}', 
                            '{record['TypeName']}', 
                            {record['Price_euros']})
                    """

            cur.execute(query)

    conn.commit()

    cur.close()
    conn.close()

@task
def display_data(filepaths: list):
    """Display data from all found files."""
    for filepath in filepaths:
        print(f"\n{'='*50}")
        print(f"Displaying data from: {filepath}")
        print(f"{'='*50}")
        df = pd.read_csv(filepath, usecols=FILE_COLS)
        print(df)

@task
def filter_gaming_laptops(filepaths: list):
    
    
    # for file in glob.glob(FILE_PATH):
        # df = pd.read_csv(file, usecols=FILE_COLS)
    
    for filepath in filepaths:

        df = pd.read_csv(filepath, usecols=FILE_COLS)

        gaming_laptops_df = df[df['TypeName'] == 'Gaming']
        
        gaming_laptops_df.to_csv(OUTPUT_FILE.format('gaming_laptops'), 
            mode='a', header=False, index=False)

@task
def filter_notebook_laptops(filepaths: list):
    for filepath in filepaths:

        df = pd.read_csv(filepath, usecols=FILE_COLS)

        notebook_laptops_df = df[df['TypeName'] == 'Notebook']

        notebook_laptops_df.to_csv(OUTPUT_FILE.format('notebook_laptops'), 
            mode='a', header=False, index=False)

@task
def filter_ultrabook_laptops(filepaths: list):
    for filepath in filepaths:

        df = pd.read_csv(filepath, usecols=FILE_COLS)

        ultrabook_laptops_df = df[df['TypeName'] == 'Ultrabook']

        ultrabook_laptops_df.to_csv(OUTPUT_FILE.format('ultrabook_laptops'), 
            mode='a', header=False, index=False)

@task
def delete_files(filepaths: list):
    import os
    for filepath in filepaths:
        os.remove(filepath)
        print(f"Deleted file: {filepath}")

with DAG(
    dag_id = 'simple_file_sensor_02',
    description = 'Running a simple file sensor',
    default_args = default_args,
    tags = ['python', 'sensor', 'file sensor'],
    template_searchpath = '/workspaces/apache-airflow-course/sql_statements'
) as dag:

    create_table_laptop = SQLExecuteQueryOperator(
        task_id = 'create_table_laptop',
        conn_id = 'postgres_connection',
        sql = 'create_table_laptops.sql'
    )

    # ✅ RECOMMENDED - use the built-in sensor, cannot be refactored into @task
    checking_for_file = FileSensor(
        task_id = 'checking_for_file',
        filepath = FILE_PATH,
        poke_interval = 10,
        timeout = 60 * 10
    )


    found_files = find_file();
    display_task = display_data(found_files);
    insert_task = insert_laptop_data(found_files);
    filter_gaming_task = filter_gaming_laptops(found_files);
    filter_notebook_task = filter_notebook_laptops(found_files);
    filter_ultrabook_task = filter_ultrabook_laptops(found_files);

    checking_for_file >> found_files >>  display_task 
    create_table_laptop >> insert_task >> [filter_gaming_task, filter_notebook_task, filter_ultrabook_task] >> delete_files(found_files)
