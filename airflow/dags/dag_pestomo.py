from click import command
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from scripts import busca_fechamento_acao, compara_valores_alertas
# import os


default_args = {
    'owner': 'guilhermedr',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0
}

# comentario
with DAG(dag_id='pipeline_pestomo',
         start_date=datetime(2022, 9, 21),
         max_active_runs=7,
         schedule_interval="0 19 * * *", 
         default_args=default_args,
         catchup=True,  # enable if you don't want historical dag runs to run
         tags=["pestomo"]
         ) as dag:

    task_1 = PythonOperator(
        task_id = 'busca_fechamento_acao',
        python_callable=busca_fechamento_acao.main
    )

    task_2 = PythonOperator(
        task_id = 'compara_valores',
        python_callable=compara_valores_alertas.main
    )

    task_1 >> task_2