from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models import Variable
from airflow.hooks.http_hook import HttpHook


def mostrar_ruta():
    ruta = Variable.get("ruta_de_dags")
    print(f"La ruta configurada de los dags es: {ruta}")
    return ruta

def llamar_api():
    http = HttpHook(http_conn_id="mi_api_http", method="GET")
    response = http.run("/")
    print("Status code:", response.status_code)
    print("Respuesta:", response.text)


with DAG (
    dag_id = "dag_con_variable",
    start_date = datetime(2026,1,20),
    schedule_interval = None,
    catchup = False,
    tags=["http", "connection"]

) as dag :

    inicio = BashOperator(
        task_id="inicio",
        bash_command='echo "Inicio del DAG"'
    )

    leer_variable = PythonOperator(
        task_id="leer_variable",
        python_callable=mostrar_ruta
    )

    llamar_api_task = PythonOperator(
        task_id="llamar_api",
        python_callable=llamar_api
    )

    fin = BashOperator(
        task_id="fin",
        bash_command='echo "Fin del DAG"'
    )

    inicio >> leer_variable  >> llamar_api_task >> fin