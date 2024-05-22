from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from processing.check_update_psql_source import *
from processing.check_update_mysql_source import *

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
    'update_dwh_daily', default_args=default_args,
    max_active_runs=1, catchup=False, schedule_interval='30 2 * * *'
)

init = DummyOperator(task_id='dummy', dag=dag)

check_update_psql_source = PythonOperator(
    task_id='check_update_psql_source',
    dag=dag,
    python_callable=check_update_psql_source,
)

check_update_mysql_source = PythonOperator(
    task_id='check_update_mysql_source',
    dag=dag,
    python_callable=check_update_mysql_source
)

# init >> [check_update_psql_source, check_update_mysql_source]
init >> [check_update_psql_source]
