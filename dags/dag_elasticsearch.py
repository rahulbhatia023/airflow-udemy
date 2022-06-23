from airflow import DAG
from datetime import datetime

from airflow.operators.python import PythonOperator

from plugins.elasticsearch_plugin.hooks.elasticsearch_hook import ElasticHook
from plugins.elasticsearch_plugin.operators.postgres_to_elastic import (
    PostgresToElasticOperator,
)

default_args = {"start_date": datetime(2020, 1, 1)}


def _print_es_info():
    hook = ElasticHook()
    print(hook.info())


with DAG(
    dag_id="elasticsearch",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
) as dag:
    print_es_info = PythonOperator(
        task_id="print_es_info", python_callable=_print_es_info
    )

    connections_to_es = PostgresToElasticOperator(
        task_id="connection_to_es", sql="select * from connection", index="connections"
    )

    print_es_info >> connections_to_es
