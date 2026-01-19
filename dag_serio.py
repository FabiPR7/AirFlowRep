from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable
from datetime import datetime
import os
import pandas as pd


def procesar_archivo(**context):
    archivo = context["ti"].xcom_pull(task_ids="esperar_archivo")
    print(f"Procesando archivo: {archivo}")
    df = pd.read_csv(archivo)
    print("Primeras filas del CSV:")
    print(df.head())

    context["ti"].xcom_push(key="filas_procesadas", value=len(df))


def reportar(**context):
        filas = context["ti"].xcom_pull(task_ids="procesar_archivo", key="filas_procesadas")
        print(f"Se procesaron {filas} filas del archivo.")

with DAG(
        dag_id="etl_completo",
        start_date=datetime(2026, 1, 20),
        schedule_interval=None,  # ejecución manual
        catchup=False,
        tags=["ETL", "archivo", "sensor"]
) as dag:


    esperar_archivo = FileSensor(
        task_id="esperar_archivo",
        filepath="/opt/airflow/dags/data/entrada/mi_archivo.csv",
        poke_interval=10,
        timeout=600,
        mode="reschedule"
    )
    procesar = PythonOperator(
        task_id="procesar_archivo",
        python_callable=procesar_archivo,
        provide_context=True
    )

    mover_archivo = BashOperator(

        task_id="mover_archivo",
        bash_command="mv /opt/airflow/dags/data/entrada/mi_archivo.csv /opt/airflow/dags/data/procesados/"
    )

    reporte = PythonOperator(
        task_id="reporte",
        python_callable=reportar,
        provide_context=True
    )

    esperar_archivo >> procesar >> mover_archivo >> reporte
