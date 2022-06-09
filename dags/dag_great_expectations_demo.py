from datetime import datetime

from airflow.models import DAG
from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator,
)

ge_root_dir = "/Users/rahulbhatia/Documents/intellij_workspace/great-expectations-demo/great_expectations"
default_args = {"start_date": datetime(2020, 1, 1)}

with DAG(
    dag_id="great_expectations_demo",
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
) as dag:
    ge_filesystem_validate = GreatExpectationsOperator(
        task_id="ge_filesystem_validate",
        data_context_root_dir=ge_root_dir,
        checkpoint_name="checkpoint_filesystem_demo",
        return_json_dict=True,
    )

    ge_postgres_validate = GreatExpectationsOperator(
        task_id="ge_postgres_validate",
        data_context_root_dir=ge_root_dir,
        checkpoint_name="checkpoint_postgres_demo",
        return_json_dict=True,
    )

    ge_filesystem_validate >> ge_postgres_validate
