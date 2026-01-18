from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from datetime import datetime, timedelta

with DAG(
        dag_id='dag_sensor_simple',
        start_date=datetime(2026, 1, 15),
        schedule_interval=None,
        catchup=False
) as dag:

    inicio = BashOperator(
        task_id='inicio',
        bash_command='echo "Inicio del DAG"'
    )

    sensor_poke = TimeDeltaSensor(
        task_id='sensor_poke',
        delta=timedelta(seconds=30),
        poke_interval=5,
        timeout=60,
        mode='poke'
    )

    sensor_reschedule = TimeDeltaSensor(
        task_id='sensor_reschedule',
        delta=timedelta(seconds=30),
        poke_interval=5,
        timeout=60,
        mode='reschedule'
    )

    fin = BashOperator(
        task_id='fin',
        bash_command='echo "Sensores completados"'
    )

    inicio >> sensor_poke >> sensor_reschedule >> fin
