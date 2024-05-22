from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from processing.ingest_psql_source import ingest_psql_source
from processing.ingest_mysql_source import ingest_mysql_source

default_args = {
    'owner': 'data_team',
    'start_date': datetime(2022, 11, 28),
    'email': ['gp.data@galaxy.com.vn'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3)
}

dag = DAG(
    'get_raw_data_from_sources', default_args=default_args,
    max_active_runs=1, catchup=False, schedule_interval='0 2 * * *'
)

init = DummyOperator(task_id='dummy', dag=dag)

get_raw_data_from_postgres_source = PythonOperator(
    task_id='get_raw_data_from_postgres_sources',
    dag=dag,
    python_callable=ingest_psql_source,
)

get_raw_data_from_mysql_source = PythonOperator(
    task_id='get_raw_data_from_mysql_sources',
    dag=dag,
    python_callable=ingest_mysql_source
)

init >> [get_raw_data_from_postgres_source, get_raw_data_from_mysql_source]
