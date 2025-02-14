import os
from pathlib import Path


DBT_HOME = Path(os.getenv('DBT_HOME'))


def dbt_path_str(dbt_model):
  path = DBT_HOME / dbt_model
  return str(path)