from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def suma():
  return  5 + 5

def obtenerResultado(ti):
    ruta = ti.xcom_pull(task_ids='tarea_generar_ruta')
    print(f"La suma 10 + 10 es : {ruta}")


with DAG (
        'dag_xcom',
        start_date=datetime(2026,1,15),
        schedule_interval='@hourly',
        catchup=False
) as dag :
    t1 = PythonOperator(
        task_id='tarea_generar_ruta',
        python_callable= suma
    )

    t2 = PythonOperator(
        task_id='tarea_recibida',
        python_callable= obtenerResultado
    )
    t1 >> t2
