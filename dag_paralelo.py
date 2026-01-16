from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
        'dag_paralelo',
        start_date=datetime(2026,1,15),
        schedule_interval=None,  # manual
        catchup=False
) as dag:

    t1 = BashOperator(
        task_id='inicio',
        bash_command='echo "Inicio del DAG"'
    )

    t2 = BashOperator(
        task_id='rama_1',
        bash_command='echo "Esta es la rama 1"'
    )

    t3 = BashOperator(
        task_id='rama_2',
        bash_command='echo "Esta es la rama 2"'
    )

    t4 = BashOperator(
        task_id='rama_3',
        bash_command='echo "Esta es la rama 3"'
    )

    t5 = BashOperator(
        task_id='fin',
        bash_command='echo "Fin del DAG"'
    )

    t1 >> [t2, t3, t4]
    [t2, t3, t4] >> t5
