import json
from datetime import datetime
from airflow.models import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id='api_dag',
    schedule_interval='@daily',
    start_date= days_ago(1),
    catchup=False
) as dag:
    
    task_is_api_active = HttpSensor(
        task_id='is_api_active',
        http_conn_id='mock-data-server-connection',
        endpoint='users'
    )

    task_get_users = SimpleHttpOperator(
        task_id='get_users',
        http_conn_id='mock-data-server-connection',
        endpoint='users',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True

    )

task_is_api_active >> task_get_users