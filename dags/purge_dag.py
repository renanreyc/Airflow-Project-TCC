import os

from datetime import datetime

# airflow
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
# from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
from airflow.decorators import task
from airflow.models import Variable

# util
from util import datalake
from util.data import purge_file

RAW_PATH = '/raw_data/'
PDS_PATH = '/processed_data/'
PDS_CLEAN_PATH = '/processed_data/data/'
PDS_TMP_PATH = '/processed_data/tmp/'

IDS_CLEAN_PATH = '/integrated_data/datasets/'
IDS_MERGE_PATH = '/integrated_data/merges/'
IDS_HIST_MERGE_PATH = '/integrated_data/merges/historical/'

# labels dos metadados
SCHEMA_LABEL = 'type'
FILE_ID_LABEL = 'id'
FILE_NAME_LABEL = 'filename'
PDSA_DATE_LABEL = 'pdsa_upload_date'

default_args = {
    'dag_id': 'purge_dag',
    'depends_on_past': False,
    'owner': 'Renan Rey',
    'start_date': days_ago(1),
}

# get_most recent_database
with DAG(default_args['dag_id'], schedule_interval='@monthly',
         default_args=default_args, catchup=False) as dag:

    start = DummyOperator(task_id='start')

    with TaskGroup('ConfigEnv') as Config:
        _user = os.getenv('USER')
        _key = os.getenv('KEY')
        _fs = os.getenv('FS')

        dtime = str(int(datetime.now().timestamp()))
        number_days_limit_purge = int(Variable.get("number_days_limit_purge"))

        metadados = {
            "user": "renan",
            "schema": "all files",
            "action": "purge files",
        }

    with TaskGroup('purge_processed_data') as purge_processed_data:
        client = datalake.DataLake(_user, _key, _fs)

        @task()
        def purge_files(client, file_day_deadline):
            files_purge_list = purge_file.purge_older_files(
                client, PDS_CLEAN_PATH, PDSA_DATE_LABEL, file_day_deadline)

            return files_purge_list

        files_purge_list = purge_files(
            client, number_days_limit_purge)

    end = DummyOperator(task_id='end')

start >> Config >> purge_processed_data >> end
