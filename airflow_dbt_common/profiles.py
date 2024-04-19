import os
from cosmos import ProfileConfig
from cosmos.profiles import RedshiftUserPasswordProfileMapping
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

DEFAULT_SP_SCHEMA=os.getenv('DEFAULT_SP_SCHEMA')
DBT_TARGET_NAME=os.getenv('DBT_TARGET_NAME')
SP_CONNECTION_NAME=os.getenv('SP_CONNECTION_NAME')


def _build_profile(schema):
  connection = BaseHook.get_connection(SP_CONNECTION_NAME)
  profile_template = '''
    default:
      outputs:
        production:
          dbname: {db_name}
          host: {db_host}
          password: {db_password}
          port: {db_port}
          schema: {schema}
          threads: 1
          type: redshift
          user: {db_user}
      target: production
  '''.format(
    db_name=connection.schema,
    db_host=connection.host,
    db_password=connection.password,
    db_port=connection.port,
    db_user=connection.login,
    schema=schema
  )
  return profile_template


def _write_temporary_profile(profile_file, schema):
  with open(profile_file, 'w') as fout:
    fout.write(_build_profile(schema))


def _remove_profile(profile_file):
  os.remove(profile_file)


def write_profile_op(profile_file, dag, schema='atomic'):
  op = PythonOperator(
    task_id="create_profile",
    python_callable=_write_temporary_profile,
    op_kwargs= { 'profile_file': profile_file, 'schema': schema },
    dag=dag
  )
  return op


def destroy_profile_op(profile_file, dag):
  op = PythonOperator(
    task_id="destroy_profile",
    python_callable=_remove_profile,
    op_kwargs= { 'profile_file': profile_file },
    dag=dag
  )
  return op


def build_profile_config(schema=None, 
    connection_name=None, 
    target_name=None
  ):
  schema = DEFAULT_SP_SCHEMA if schema is None else schema
  connection_name = SP_CONNECTION_NAME if connection_name is None else connection_name
  target_name = DBT_TARGET_NAME if target_name is None else target_name
  return ProfileConfig(
    profile_mapping= connection_name,
    target_name=target_name,
    profile_mapping=RedshiftUserPasswordProfileMapping(
      conn_id=connection_name,
      profile_args={'schema': schema}
    )
  )

snowplow_db = build_profile_config()
# ^^ builds default profile VV
# snowplow_db = ProfileConfig(
#   profile_name=SP_CONNECTION_NAME,
#   target_name=DBT_TARGET_NAME,
#   profile_mapping=RedshiftUserPasswordProfileMapping(
#     conn_id=SP_CONNECTION_NAME,
#     profile_args={'schema': DEFAULT_SP_SCHEMA}
#   )
# )