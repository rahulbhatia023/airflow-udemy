from datetime import datetime

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

default_args = {"start_date": datetime(2020, 1, 1)}

with DAG(
    dag_id="parallel_dag",
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
) as dag:
    task_1 = BashOperator(
        task_id="task_1",
        bash_command="sleep 3",
    )

    with TaskGroup(group_id="processing_task_group") as processing_task_group:
        task_2 = BashOperator(
            task_id="task_2",
            bash_command="sleep 3",
        )

        task_3 = BashOperator(
            task_id="task_3",
            bash_command="sleep 3",
        )

    task_4 = BashOperator(
        task_id="task_4",
        bash_command="sleep 3",
    )

    task_1 >> processing_task_group >> task_4
