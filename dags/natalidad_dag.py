from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests

default_args = {
    'owner': 'paula-alvarado',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def descargar_datos_ejemplo(**kwargs):
    """Función de ejemplo - después la reemplazamos"""
    print("✅ Descargando datos de natalidad...")
    # Acá va el código de descarga
    return "datos_descargados"

def procesar_datos(**kwargs):
    """Función de ejemplo - después la reemplazamos"""
    print("✅ Procesando datos...")
    # Acá va el código de procesamiento
    return "datos_procesados"

with DAG(
    'natalidad_argentina',
    default_args=default_args,
    schedule_interval='@monthly',  # Se ejecuta mensualmente o despues definimos como
    catchup=False,
    tags=['natalidad', 'argentina', 'datos']
) as dag:

    descargar_task = PythonOperator(
        task_id='descargar_datos',
        python_callable=descargar_datos_ejemplo
    )

    procesar_task = PythonOperator(
        task_id='procesar_datos',
        python_callable=procesar_datos
    )

    # Definir dependencias
    descargar_task >> procesar_task