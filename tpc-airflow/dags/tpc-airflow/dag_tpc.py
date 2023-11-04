import os
import json
import papermill as pm
from airflow import DAG
from airflow.models import Variable, Connection
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator


# Definição dos parâmetros da DAG
default_args = {
    "owner": "tpc",                     #Proprietário da DAG
    "depends_on_past": False,           #Indica se a DAG depende de uma execução anterior
    "start_date": datetime(year=2023, month=8, day=28), #Data de início de execução da DAG
    "email": [""],                      #Email para notificação
    "email_on_failure": False,          #Indica se o email será enviado em caso de falha
    "email_on_retry": False,            #Indica se o email será enviado em caso de retry
    "retries": 1,                       #Número de tentativas de execução
    "retry_delay": timedelta(seconds=5),#Tempo entre as tentativas
    "schedule_interval": "None",      #Intervalo de execução da DAG
}

DAG_NAME = "dag_tpc"
DAG_DESCRIPTION = "DAG TESTES SINTÉTICOS"
# File path
NB_TPC = "/opt/airflow/dags/tpc-airflow/spark_jobs/TPC.ipynb"

# Conn Ids
CONN_SOURCE = Connection.get_connection_from_secrets("SOURCE")
CONN_TARGET = Connection.get_connection_from_secrets("TARGET")

# Instanciação da DAG
dag = DAG(
    DAG_NAME, 
    default_args=default_args,
    description=DAG_DESCRIPTION,
    catchup=False,
    # schedule_interval= timedelta(hours=12)
)

# Get connection data
NB_PARAMETERS = {
    "interative": False,
    "SOURCE_USER": CONN_SOURCE.login,
    "SOURCE_PASS": CONN_SOURCE.password,
    "SOURCE_DB": CONN_SOURCE.schema,
    "SOURCE_HOST": CONN_SOURCE.host,
    "SOURCE_PORT": CONN_SOURCE.port,
    "TARGET_USER": CONN_TARGET.login,
    "TARGET_PASS": CONN_TARGET.password,
    "TARGET_DB": CONN_TARGET.schema,
    "TARGET_HOST": CONN_TARGET.host,
    "TARGET_PORT": CONN_TARGET.port
}

with dag:
    t1 = PythonOperator(
        task_id="TPC",
        python_callable=pm.execute_notebook,
        op_kwargs={
            "input_path": NB_TPC,
            "output_path": None,
            "parameters": NB_PARAMETERS,
            "log_output": True,
        }
    )
    
    t1