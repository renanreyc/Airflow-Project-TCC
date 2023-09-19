from io import BytesIO
import fsspec
import pandas as pd
from datetime import datetime
import os


# airflow
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.exceptions import AirflowFailException
from airflow.utils.dates import days_ago
from airflow.decorators import task, task_group
from airflow.models import Variable
from airflow.utils.edgemodifier import Label
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# util
from util import datalake

# paths
PDS_CLEAN_PATH = '/processed_data/data/'
PDS_TMP_PATH = '/processed_data/tmp/'

IDS_CONCAT_PATH = '/integrated_data/concat/'
IDS_MERGE_OUTPUT_PATH = 'integrated_data/merge/output/'
IDS_HIST_MERGE_OUTPUT_PATH = 'integrated_data/merge/output/historical/'
IDS_HIST_CONCAT_PATH = '/integrated_data/concat/historical/'

MERGE_RATE_PLANATTRIBUTES = 'merge_output'

# labels dos metadados
SCHEMA_LABEL = 'type'
FILE_ID_LABEL = 'id'
FILE_NAME_LABEL = 'filename'
EXTRACTION_DATE_LABEL = 'extraction_date'

# extras 
EXTENSION_DFT = '.parquet'

default_args = {
    'start_date': days_ago(1),
    'dag_id': 'merge_dag',
    'next_dag': 'report_dag',
    'depends_on_past': False,
    'owner': 'Renan Rey'
}

with DAG(default_args['dag_id'], schedule_interval=None,
         default_args=default_args, catchup=False) as dag:

    start = DummyOperator(task_id='start')

    with TaskGroup('ConfigEnv') as Config:
        _user = os.getenv('USER')
        _key = os.getenv('KEY')
        _fs = os.getenv('FS')

        dtime = str(int(datetime.now().timestamp()))

        # tables
        tables = {
            "BenefitsCostSharing": {
                # 2016 removed two fields: 'IsSubjToDedTier2', 'IsSubjToDedTier1
                "2016": "BenefitsCostSharing_2016.parquet",
                "2015": "BenefitsCostSharing_2015.parquet",
                "2014": "BenefitsCostSharing_2014.parquet"
            },
            "BusinessRules": {
                # 2016 renamed DentalOnly => DentalOnlyPlan
                "2016": "BusinessRules_2016.parquet",
                "2015": "BusinessRules_2015.parquet",
                "2014": "BusinessRules_2014.parquet"
            },
            "Crosswalk2015": {
                "data": "Crosswalk2015_data.parquet"
            },
            "Crosswalk2016": {
                "data": "Crosswalk2016_data.parquet"
            },
            "Network": {
                # 2016 renamed DentalOnly => DentalOnlyPlan
                "2016": "Network_2016.parquet",
                "2015": "Network_2015.parquet",
                "2014": "Network_2014.parquet"
            },
            "PlanAttributes": {
                # This is complicated - lots of fields change b/t 2014/2015 & 2016
                "2016": "PlanAttributes_2016.parquet",
                "2015": "PlanAttributes_2015.parquet",
                "2014": "PlanAttributes_2014.parquet"
            },
            "Rate": {
                # 2016 same structure as 2015 and 2014
                "2016": "Rate_2016.parquet",
                "2015": "Rate_2015.parquet",
                "2014": "Rate_2014.parquet"
            },
            "ServiceArea": {
                # 2016 renamed DentalOnly => DentalOnlyPlan
                "2016": "ServiceArea_2016.parquet",
                "2015": "ServiceArea_2015.parquet",
                "2014": "ServiceArea_2014.parquet"
            }
        }

        metadados = {
            "user": "data_team",
            "schema": tables,
            "action": "concat data to prediction",
        }
        # PGSQL VARIABLES
        back_pg_host = Variable.get("back_pg_host")
        back_pg_port = Variable.get("back_pg_port")
        back_pg_dbname = Variable.get("back_pg_dbname")
        back_pg_user = Variable.get("back_pg_user")
        back_pg_password = Variable.get("back_pg_password")

    with TaskGroup('concat_files') as concat_files:
        client = datalake.DataLake(_user, _key, _fs)
        @task()
        def concat(client, tables):
            md_results = []
            for table in tables:
                print('>>>>>>>>>>' + str(table))

                if table[:9] != "Crosswalk":
                    buffer, _, md = client.get_file(PDS_CLEAN_PATH + table, tables[table]["2014"])
                    d2014 = pd.read_parquet(BytesIO(buffer))
                    buffer, _, md = client.get_file(PDS_CLEAN_PATH + table, tables[table]["2015"])
                    d2015 = pd.read_parquet(BytesIO(buffer))
                    buffer, _, md = client.get_file(PDS_CLEAN_PATH + table, tables[table]["2016"])
                    d2016 = pd.read_parquet(BytesIO(buffer))
                    md_file = md['metadata']


                    if table in ["ServiceArea", "BusinessRules", "Network"]:
                        d2015 = d2015.rename(columns={"DentalOnly":"DentalOnlyPlan"})
                        d2014 = d2014.rename(columns={"DentalOnly":"DentalOnlyPlan"})

                    if table=="BenefitsCostSharing":
                        d2015 = d2015.rename(columns={"EHBPercentPremiumS4":"EHBPercentTotalPremium"})
                        d2014 = d2014.rename(columns={"EHBPercentPremiumS4":"EHBPercentTotalPremium"})

                    if table != "Crosswalk":
                        df = pd.concat([d2014, d2015, d2016])
                    else:
                        df = pd.concat([d2015, d2016])
                else:
                    buffer, _, md = client.get_file(PDS_CLEAN_PATH + table, tables[table]["data"])
                    df = pd.read_parquet(BytesIO(buffer))
                    md_file = md['metadata']

                # update new metadatas
                new_metadata = md_file
                dt = datetime.now()
                dt_str = str(dt.year) + '-' + str(dt.month) + '-' + str(dt.day)
                historical_concat_file_name = table + '_' + dt_str + EXTENSION_DFT

                update_metadados = {
                    'action': 'concat file create',
                    'new_filename': historical_concat_file_name,
                    'type': 'concat',
                    'merge_date': dt_str
                }
                new_metadata.update(update_metadados)

                client.upload_data_frame(
                    df, IDS_CONCAT_PATH, table + EXTENSION_DFT, file_metadata=new_metadata, remote_output_format='parquet')
                md_results.append(new_metadata)

                # save historical concat
                dt = datetime.now()
                df['CONCAT_DATE'] = int(round(dt.timestamp() * 1000))
                client.upload_data_frame(
                    df, IDS_HIST_CONCAT_PATH, historical_concat_file_name, file_metadata=new_metadata,remote_output_format='parquet')

        results_concats = concat(client, tables)

    with TaskGroup('merge_data') as merge_data:
        
        @task
        def merge_rate_planattributes():
            buffer, _, md = client.get_file('integrated_data/concat/', 'Rate.parquet')
            df_rate = pd.read_parquet(BytesIO(buffer))
            md_rate = md.metadata
            
            buffer, _, md = client.get_file('integrated_data/concat/', 'PlanAttributes.parquet')
            df_plan = pd.read_parquet(BytesIO(buffer))
            md_plan = md.metadata

            md_rate.update(md_plan)

            # Unindo os dataframes
            merged_df = pd.merge(df_rate, df_plan, left_on=df_rate['RATE_PLANID'], right_on=df_plan['PLANATTRIBUTES_PLANID'].str[:14], how='inner')
            filtered_df = merged_df[(merged_df['PLANATTRIBUTES_DENTALONLYPLAN'] == 'Yes') & (merged_df['RATE_INDIVIDUALRATE'] != 999999)]
            grouped_df = filtered_df.groupby(['RATE_STATECODE', 'RATE_BUSINESSYEAR']).agg({'RATE_INDIVIDUALRATE': 'mean'}).reset_index()
            sorted_df = grouped_df.sort_values('RATE_STATECODE')

            # update metadata
            dt = datetime.now()
            dt_str = str(dt.year) + '-' + str(dt.month) + '-' + str(dt.day)
            update_metadados = {
                    'action': 'merge file create',
                    'new_filename': MERGE_RATE_PLANATTRIBUTES + EXTENSION_DFT,
                    'type': 'merge',
                    'merge_date': dt_str
                }
            md_rate.update(update_metadados)

            client.upload_data_frame(
                sorted_df, IDS_MERGE_OUTPUT_PATH, MERGE_RATE_PLANATTRIBUTES + EXTENSION_DFT, file_metadata=md_rate, remote_output_format='parquet')
    
            # save historical merge
            sorted_df['MERGE_DATE'] = int(round(dt.timestamp() * 1000))
            client.upload_data_frame(
                sorted_df, IDS_HIST_MERGE_OUTPUT_PATH, MERGE_RATE_PLANATTRIBUTES + EXTENSION_DFT, file_metadata=md_rate,remote_output_format='parquet')
    
        
        results_merge_rate_and_planattributes = merge_rate_planattributes()

    # invoke de predicion dag
    exec_pred = TriggerDagRunOperator(
        task_id='trigger_report_dag',
        trigger_dag_id=default_args['next_dag'],
        execution_date='{{ ds }}',
        reset_dag_run=True,
    )

    end = DummyOperator(task_id='end')

start >> Config >> concat_files >> merge_data >> exec_pred >> end
