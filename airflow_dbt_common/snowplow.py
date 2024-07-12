from airflow.operators.bash import BashOperator
from airflow import DAG
import warnings
import functools

def deprecated(reason):
    """
    This decorator is used to mark functions as deprecated. It will result in a warning being emitted when the function is used.
    :param reason: A string describing the reason for deprecation and, if applicable, the alternative function to use.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            warnings.warn(
                f"{func.__name__}() is deprecated: {reason}",
                DeprecationWarning,
                stacklevel=2
            )
            return func(*args, **kwargs)
        return wrapper
    return decorator

@deprecated('use dbt.py run_dbt as we are abandoning the cosmos module - it is too slow and fucks up running some dags. maybe revisit at some point')
def run_snowplow_web(
#NB: run dbt as a bash operator until cosmos supports --selector
  cmd: str,  
  dag: DAG, 
  task_id: str)->BashOperator:
  op = BashOperator(
    dag=dag,
    task_id=task_id,
    bash_command=cmd
  )
  return op
