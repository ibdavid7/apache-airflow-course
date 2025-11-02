import pandas as pd
from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook  # ✅ Use GCS Hook
from io import StringIO

default_args = {
   'owner': 'loonycorn'
}

GCS_CONNECTION_ID = 'gcs_connection'  # ✅ GCP connection ID

DEFAULT_START_DATE = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=5)

def read_gcs_file(bucket_name, object_name):
    """Read file from GCS bucket"""
    import os
    
    # Verify ADC is set (educational - shows students what to check)
    if not os.getenv('GOOGLE_APPLICATION_CREDENTIALS'):
        raise ValueError(
            "GOOGLE_APPLICATION_CREDENTIALS not set. "
            "Export the environment variable pointing to your service account key."
        )
        
    gcs_hook = GCSHook(gcp_conn_id=GCS_CONNECTION_ID)  # ✅ GCP connection

    # Download file content
    file_content = gcs_hook.download(
        bucket_name=bucket_name,
        object_name=object_name
    )
    
    if isinstance(file_content, bytes):
        file_content = file_content.decode('utf-8')
    
    df = pd.read_csv(StringIO(file_content))
    return df.to_json()

def remove_null_values(json_data):
    df = pd.read_json(json_data)
    
    df = df.dropna()
    
    return df.to_json()

def create_table_customer_credit_card_details():
    pg_hook = PostgresHook(postgres_conn_id='postgres_connection')
    
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS customer_credit_card_details (
            id INT,
            name VARCHAR(255),
            email VARCHAR(255),
            credit_card_number VARCHAR(50),
            credit_card_type VARCHAR(50)
        );
    """

    pg_hook.run(sql=create_table_query)

def insert_data_customer_credit_card_details(json_data):
    df = pd.read_json(json_data)

    pg_hook = PostgresHook(postgres_conn_id='postgres_connection')

    for _, row in df.iterrows():
        insert_query = f"""
            INSERT INTO customer_credit_card_details 
            (id, name, email, credit_card_number, credit_card_type)
            VALUES ({row['id']}, 
                    '{row['name']}', 
                    '{row['email']}', 
                    '{row['credit card number']}', 
                    '{row['credit card type']}');
        """

        pg_hook.run(sql=insert_query)


with DAG(dag_id='pipeline_with_gcs_hook',
         description='Executing a pipeline with a GCS hook',
         default_args=default_args,
         start_date=DEFAULT_START_DATE,
         schedule='@once',
         tags=['python', 'hooks', 'postgres hook', 'gcs hook', 'gcs bucket']
) as dag:

    read_gcs_file_task = PythonOperator(
        task_id='read_gcs_file_task',
        python_callable=read_gcs_file,
        op_kwargs={
            'bucket_name': 'mrfrepo',
            'object_name': 'temp/credit_card_details.csv'
        }
    )

    remove_null_values_task = PythonOperator(
        task_id='remove_null_values_task',
        python_callable=remove_null_values,
        op_kwargs={
            'json_data': '{{ ti.xcom_pull(task_ids="read_gcs_file_task") }}'
        }
    )

    create_table_customer_credit_card_details_task = PythonOperator(
        task_id='create_table_customer_credit_card_details_task',
        python_callable=create_table_customer_credit_card_details
    )

    insert_data_customer_credit_card_details_task = PythonOperator(
        task_id='insert_data_customer_credit_card_details_task',
        python_callable=insert_data_customer_credit_card_details,
        op_kwargs={
            'json_data': '{{ ti.xcom_pull(task_ids="remove_null_values_task") }}'
        }
    )

    read_gcs_file_task >> remove_null_values_task >> \
    create_table_customer_credit_card_details_task >> \
    insert_data_customer_credit_card_details_task











