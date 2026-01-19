from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
        'dag_secuencial3',
        start_date=datetime(2026,1,15),
        schedule_interval='@hourly',
        catchup=False
) as dag:

    t1 = BashOperator(
        task_id='sec_Inicio',
        bash_command='echo "Inicio del DAG"'
    )

    t2 = BashOperator(
        task_id='sec_2',
        bash_command='echo "Esta es la rama 1"'
    )

    t3 = BashOperator(
        task_id='sec_3',
        bash_command='echo "Esta es la rama 2"'
    )


    t1 >> t2 >> t3
