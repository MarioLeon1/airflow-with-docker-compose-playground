from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests

# URLs de ejemplo para las descargas
DATA_URL1 = "https://raw.githubusercontent.com/datasets/population/master/data/population.csv"
DATA_URL2 = "https://raw.githubusercontent.com/datasets/gdp/master/data/gdp.csv"
MERGED_PATH = "/tmp/merged_data.csv"
REPORT_PATH = "/tmp/combined_report.txt"

# Funciones de procesamiento
def download_data_1():
    response = requests.get(DATA_URL1)
    with open("/tmp/raw_data1.csv", "w") as f:
        f.write(response.text)

def download_data_2():
    response = requests.get(DATA_URL2)
    with open("/tmp/raw_data2.csv", "w") as f:
        f.write(response.text)

def merge_data():
    df1 = pd.read_csv("/tmp/raw_data1.csv")
    df2 = pd.read_csv("/tmp/raw_data2.csv")
    # Hacer un merge basado en algún campo común, por ejemplo "Country Name"
    merged_df = pd.merge(df1, df2, on="Country Name", how="inner")
    merged_df.to_csv(MERGED_PATH, index=False)

def generate_combined_report():
    df = pd.read_csv(MERGED_PATH)
    with open(REPORT_PATH, "w") as f:
        f.write("Reporte Combinado\n")
        f.write("=================\n\n")
        f.write(df.to_string())

# Definición del DAG
default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'parallel_download_and_merge',
    default_args=default_args,
    description='DAG de descargas paralelas, merge y generación de reporte',
    schedule_interval=timedelta(days=1),
)

# Tareas
download_task_1 = PythonOperator(
    task_id='download_data_1',
    python_callable=download_data_1,
    dag=dag,
)

download_task_2 = PythonOperator(
    task_id='download_data_2',
    python_callable=download_data_2,
    dag=dag,
)

merge_task = PythonOperator(
    task_id='merge_data',
    python_callable=merge_data,
    dag=dag,
)

report_task = PythonOperator(
    task_id='generate_combined_report',
    python_callable=generate_combined_report,
    dag=dag,
)

# Definir dependencias
[download_task_1, download_task_2] >> merge_task >> report_task

