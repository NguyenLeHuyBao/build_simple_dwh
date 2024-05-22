from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2022, 5, 31),
    'email': ['gp.data@galaxy.com.vn'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'auto_partition_table', default_args=default_args,
    max_active_runs=1, catchup=False, schedule_interval='00 18 1 */4 *')

dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag)

show_logs = BashOperator(
    task_id='test_show_logs',
    bash_command='echo Hello World',
    dag=dag
)

dummy_operator >> show_logs
