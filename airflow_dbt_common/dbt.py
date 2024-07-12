from airflow.hooks.base import BaseHook
from airflow.operators.bash import BashOperator
from airflow import DAG
import os

def run_dbt(
  cmd: str,
  dag: DAG,
  task_id: str,
  connection_name: str,
  schema: str
)->BashOperator:
  # Assume the profiles.yml file is expecting environ variables, so make them available
  connection = BaseHook.get_connection(connection_name)
  os.environ['DBT_ENV_SECRET_DB'] = connection.schema
  os.environ['DBT_ENV_SECRET_DB_HOST'] = connection.host
  os.environ['DBT_ENV_SECRET_DB_PASSWORD'] = connection.password
  os.environ['DBT_ENV_SECRET_DB_SCHEMA'] = schema
  os.environ['DBT_ENV_SECRET_DB_USERNAME'] = connection.login
  op = BashOperator(
    dag=dag,
    task_id=task_id,
    bash_command=cmd
  )
  return op