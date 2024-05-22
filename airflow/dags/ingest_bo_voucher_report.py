from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from processing.ingest_bo_voucher_report import ingest_bo_voucher_report


default_args = {
    'owner': 'data_team',
    'start_date': datetime(2023, 2, 20),
    'email': ['gp.data@galaxy.com.vn'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3)
}

dag = DAG(
    'ingest_bo_voucher_report', default_args=default_args,
    max_active_runs=1, catchup=False, schedule_interval='0 1 * * *'
)

init = DummyOperator(task_id='dummy', dag=dag)

ingest_bo_voucher_report = PythonOperator(
    task_id='ingest_bo_voucher_report',
    dag=dag,
    python_callable=ingest_bo_voucher_report,
)

init >> ingest_bo_voucher_report