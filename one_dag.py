from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    'mi_primer_dag',
    start_date=datetime(2026,1,15),
    schedule_interval=None,
    catchup=False
) as dag:

    t1 = BashOperator(
        task_id='tarea_1',
        bash_command='echo "Hola Airflow!"'
    )
