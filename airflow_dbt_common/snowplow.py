from airflow.operators.bash import BashOperator
from airflow import DAG

#NB: run dbt as a bash operator until cosmos supports --selector
def run_snowplow_web(
  cmd: str,  
  dag: DAG, 
  task_id: str)->BashOperator:
  op = BashOperator(
    dag=dag,
    task_id=task_id,
    bash_command=cmd
  )
  return op