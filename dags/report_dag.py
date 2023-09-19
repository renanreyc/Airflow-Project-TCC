from io import BytesIO
import pandas as pd
from datetime import datetime

import os

# airflow
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.exceptions import AirflowFailException
from airflow.utils.dates import days_ago
from airflow.decorators import task
from airflow.models import Variable

# util
from util import datalake
from util.connectors import BackendDatabase

# paths
IDS_CLEAN_PATH = '/integrated_data/datasets/'
IDS_MERGE_PATH = '/integrated_data/merges/'
IDS_HIST_MERGE_PATH = '/integrated_data/merges/historical/'
IDS_CONCAT_PATH = 'integrated_data/concat/'
IDS_MERGE_OUTPUT_PATH = 'integrated_data/merge/output/'

# labels dos metadados
SCHEMA_LABEL = 'type'
FILE_ID_LABEL = 'id'
FILE_NAME_LABEL = 'filename'


default_args = {
    'dag_id': 'report_dag',
    'parent_dag': 'merge_dag',
    'parent_dag_task': 'end',
    'depends_on_past': False,
    'owner': 'Renan Rey',
    'start_date': days_ago(1),
}

def load_generic(path, name_file):
    back_end_connection = BackendDatabase(
        back_pg_host, back_pg_port, back_pg_dbname, back_pg_user, back_pg_password)
    buffer, _, _ = client.get_file(path, name_file)
    df = pd.read_parquet(BytesIO(buffer))

    name_table = name_file.split('.')[0].lower()

    script_creation = back_end_connection.generate_create_table(df, name_table)
    back_end_connection.execute_create_table(script_creation, name_table)
    results_insert = back_end_connection.execute_values(df, name_table)

    return results_insert


with DAG(default_args['dag_id'], schedule_interval=None,
         default_args=default_args, catchup=False) as dag:

    start = DummyOperator(task_id='start')

    with TaskGroup('ConfigEnv') as Config:
        # DATA LAKE VAR
        _user = os.getenv('USER')
        _key = os.getenv('KEY')
        _fs = os.getenv('FS')

        # PGSQL VARIABLES
        back_pg_host = Variable.get("back_pg_host")
        back_pg_port = Variable.get("back_pg_port")
        back_pg_dbname = Variable.get("back_pg_dbname")
        back_pg_user = Variable.get("back_pg_user")
        back_pg_password = Variable.get("back_pg_password")
        # configs

        dtime = str(int(datetime.now().timestamp()))

        metadados = {
            "user": "Renan Rey",
            "schema": "TCC Project",
            "action": "merge data",
        }

    with TaskGroup('load_concats') as load_concats:
        client = datalake.DataLake(_user, _key, _fs)

        @task
        def load_rate_puf():
            name_file = 'Rate.parquet'
            return load_generic(IDS_CONCAT_PATH,name_file)
        load_rate_puf()

        @task
        def load_bencs_puf():
            name_file = 'BenefitsCostSharing.parquet'
            return load_generic(IDS_CONCAT_PATH,name_file)
        load_bencs_puf()

        @task
        def load_plan_puf():
            name_file = 'PlanAttributes.parquet'
            return load_generic(IDS_CONCAT_PATH,name_file)
        load_plan_puf()

        @task
        def load_br_puf():
            name_file = 'BusinessRules.parquet'
            return load_generic(IDS_CONCAT_PATH,name_file)
        load_br_puf()

        @task
        def load_ntwrk_puf():
            name_file = 'Network.parquet'
            return load_generic(IDS_CONCAT_PATH,name_file)
        load_ntwrk_puf()

        @task
        def load_sa_puf():
            name_file = 'ServiceArea.parquet'
            return load_generic(IDS_CONCAT_PATH,name_file)
        load_sa_puf()

        def load_cw_puf():
            name_file_2016 = 'Crosswalk2016.parquet'
            result_2016 = load_generic(IDS_CONCAT_PATH,name_file_2016)
            name_file_2015 = 'Crosswalk2015.parquet'
            result_2015 = load_generic(IDS_CONCAT_PATH,name_file_2015)
            return result_2016, result_2015
        load_cw_puf()

    with TaskGroup('load_merges') as load_merges:
        client = datalake.DataLake(_user, _key, _fs)
    
        @task
        def load_merge_rate_plan():
            name_file = 'merge_rate_planattributes.parquet'
            return load_generic(IDS_MERGE_OUTPUT_PATH,name_file)
        load_merge_rate_plan()

    end = DummyOperator(task_id='end')


start >> Config >> load_concats >> load_merges >> end
