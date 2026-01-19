from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


def tarea_falla():
    raise Exception("Esta tarea falla intencionadamente")

with DAG(
        dag_id='dag_tarea_falla',
        start_date=datetime(2026,1,15),
        schedule_interval=None,
        catchup=False
) as dag:

    inicio = BashOperator(
        task_id='inicio',
        bash_command='echo "Inicio del DAG"'
    )

    falla = PythonOperator(
        task_id='tarea_falla',
        python_callable=tarea_falla,
        retries=3,
        retry_delay=timedelta(seconds=10)
    )

    fin = BashOperator(
        task_id='fin',
        bash_command='echo "Fin del DAG"',
        trigger_rule='all_done'
    )

    inicio >> falla >> fin
