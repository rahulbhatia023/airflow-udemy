from datetime import datetime

from airflow.models import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
import json

default_args = {"start_date": datetime(2020, 1, 1)}

with DAG(
    dag_id="user_processing",
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
) as dag:
    create_table = SqliteOperator(
        task_id="create_table",
        sqlite_conn_id="db_sqlite",
        sql="""
            CREATE TABLE users(
                firstname TEXT NOT NULL,
                lastname TEXT NOT NULL,
                country TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                email TEXT NOT NULL PRIMARY KEY
            );
            """,
    )

    is_api_available = HttpSensor(
        task_id="is_api_available", http_conn_id="user_api", endpoint="api/"
    )

    extracting_user = SimpleHttpOperator(
        task_id="extracting_user",
        http_conn_id="user_api",
        endpoint="api/",
        method="GET",
        response_filter=lambda response: json.loads(response.text),
        log_response=True,
    )
