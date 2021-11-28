from datetime import datetime, timedelta
from pendulum import timezone

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

with DAG(
    dag_id='date_time',
    schedule_interval='0 * * * *',
    start_date=datetime(2021, 11, 27, 22, tzinfo=timezone('Europe/Moscow')),
    end_date=datetime(2021, 11, 28, 4, tzinfo=timezone('Europe/Moscow')),
    catchup=True,
    max_active_runs=1,
    concurrency=1,
    dagrun_timeout=timedelta(minutes=60),
) as dag:
    cmd = [
        ("real_execution_time_msk", "$(date '+%Y-%m-%d %H:%M:%S')"),
        ("", ""),
        ("next_execution_date_msk", "{{ next_execution_date.in_timezone('Europe/Moscow') }}"),
        ("next_ds_msk", "{{ next_execution_date.in_timezone('Europe/Moscow').strftime('%Y-%m-%d') }}"),
        ("execution_date_msk", "{{ execution_date.in_timezone('Europe/Moscow') }}"),
        ("ds_msk", "{{ execution_date.in_timezone('Europe/Moscow').strftime('%Y-%m-%d') }}"),
        ("prev_execution_date_msk", "{{ prev_execution_date.in_timezone('Europe/Moscow') }}"),
        ("prev_ds_msk", "{{ prev_execution_date.in_timezone('Europe/Moscow').strftime('%Y-%m-%d') }}"),
        ("next_execution_date", "{{ next_execution_date }}"),
        ("next_ds", "{{ next_ds }}"),
        ("execution_date", "{{ execution_date }}"),
        ("ds", "{{ ds }}"),
        ("prev_execution_date", "{{ prev_execution_date }}"),
        ("prev_ds", "{{ prev_ds }}"),
    ]
    BashOperator(
        task_id="test",
        bash_command="\n".join(["echo {}: {}".format(task[0], task[1]) for task in cmd])
    )
