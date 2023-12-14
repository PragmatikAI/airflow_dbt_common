import os
from pathlib import Path
from cosmos import ExecutionConfig


DBT_HOME = Path(os.getenv('DBT_HOME'))

dbt_executable = Path(os.getenv('DBT_EXE'))
venv_execution_config = ExecutionConfig(
  dbt_executable_path=str(dbt_executable)
)

def dbt_path_str(dbt_model):
  path = DBT_HOME / dbt_model
  return str(path)