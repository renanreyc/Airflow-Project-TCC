import pandas as pd
from datetime import datetime
import json
import logging

import os

# airflow
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.exceptions import AirflowFailException
from airflow.utils.dates import days_ago
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.edgemodifier import Label

# util
from util import datalake
from util.connectors import BackendDatabase
from util.data import pre_processing, schema_validator, creater_analysis, handle_columns
from util import datalake
from util.data.tracking import IngestionTracker
from util.data import pre_processing, anonymization, schema_validator

# paths
RAW_PATH = '/raw_data/'
PDS_CLEAN_PATH = '/processed_data/data/'
PDS_TMP_PATH = '/processed_data/tmp/'

# labels dos metadados
SCHEMA_LABEL = 'schema'
FILE_ID_LABEL = 'id'
FILE_YEAR_LABEL = 'year'
FILE_NAME_LABEL = 'filename'
FILE_CONTENT_HASH = 'content_hash'
VALIDATION_DATE_LABEL = 'validation_date'
PDSA_UPDATE_DATE_LABEL = 'pdsa_upload_date'
PROCESSED_DATA_FORMAT_LABEL = 'processed_data_format'
NEW_FILE_NAME_LABEL = 'new_filename'

# option values
SCHEMA_LABEL_OPTIONS = ['BenefitsCostSharing', 'BusinessRules', 'Crosswalk2015',
                        'Crosswalk2016', 'Network', 'PlanAttributes', 'Rate', 'ServiceArea']
EXTENSION_OPTIONS = ['csv', 'CSV','xlsx']

default_args = {
    'dag_id': 'ingestion_dag',
    'next_dag': 'merge_dag',
    'depends_on_past': False,
    'owner': 'Renan Rey',
    'start_date': days_ago(1),
}

def read_input(client, path, metadata_file):

    filename = str(metadata_file[FILE_NAME_LABEL])
    type = str(metadata_file[SCHEMA_LABEL])

    _data, _, _ = client.get_file(path, filename)

    df = pre_processing.switch_data(
        _data, filename, type)

    return df


def read_validator(metadata_file):
    columns_dict = {}

    type = metadata_file[SCHEMA_LABEL]
    variables = [
        'column_name',
        'column_name_formatted',
        'column_percent_unique',
        'column_size',
        'column_type',
        'column_type_dataframe',
        'is_anonymizated_field',
        'is_primary_key',
        'percent_filling'
    ]

    try:
        type = metadata_file[SCHEMA_LABEL]
        for variable in variables:
            variable_airflow = Variable.get(f"{type}_{variable}")

            columns_dict[variable] = json.loads(variable_airflow)

        df_validator = pd.DataFrame(
            columns_dict, columns=columns_dict.keys())
    except Exception as e:
        error = f"Error ao ler o validator do arquivo {metadata_file[FILE_NAME_LABEL]}:\n {e}"
        print(error)

        df_validator = df_validator.rename(
            columns=lambda x: x.split('_', 1)[1])

    return df_validator


with DAG(default_args['dag_id'],
         schedule_interval='0 23 * * *',
         default_args=default_args, catchup=False) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    with TaskGroup('ConfigEnv') as Config:
        # DATALAKE VARIABLES
        # _user = Variable.get("data_lake_user")
        # _key = Variable.get("data_lake_key")
        # _fs = Variable.get("data_lake_fsname")
        
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

        dtime = str(int(datetime.now().timestamp()))
        number_days_limit_purge = 6 * 30  # 6 meses

        metadados = {
            "user": "renan",
            "schema": "undifined",
            "action": "ingest",
        }

        # errors
        metadata_error_list = []
        schema_error_list = []
        error_list = []

    with TaskGroup('list_inputs') as list_inputs:

        @task()
        def list_raw_files(client, path, **kwargs):
            correct_input_md_files, fault_input_files = pre_processing.specified_file(
                client, path, SCHEMA_LABEL_OPTIONS, EXTENSION_OPTIONS)

            ti = kwargs['ti']
            ti.xcom_push(key='fault_input_files', value=fault_input_files)

            return correct_input_md_files

        @task()
        def tracking_files(client, file_list):
            tracking = IngestionTracker()
            already_processed_files_list = []

            for i in range(len(file_list)):
                file_metadata = file_list[i]
                file_hash = file_metadata[FILE_CONTENT_HASH]
                if tracking.is_file_processed(file_hash):
                    already_processed_files_list.append(i)
                    logging.info(">>> File Already Processed")
                    logging.debug(file_metadata)
                    logging.debug("<<<")
                else:
                    # a adição e feita apenas apos o processamento final?
                    tracking.add_file(file_metadata)

            # remove os arquivos na lista
            if len(already_processed_files_list) > 0:
                for ele in sorted(already_processed_files_list, reverse=True):
                    del file_list[ele]

            return file_list

        client = datalake.DataLake(_user, _key, _fs)

        files_ok = list_raw_files(client, RAW_PATH)
        correct_input_md_files = tracking_files(client, files_ok)

    with TaskGroup('validate_schema') as validate_schema:

        @task()
        def validator_schema(correct_input_files, **kwargs):
            validate_input_files = []
            errors_filtered_list = []

            for metadata_file in correct_input_files:
                print('>>>>>>>>> Schema label in validation: ' +
                      metadata_file[SCHEMA_LABEL])
                validate_results = []
                try:
                    df = read_input(client, RAW_PATH + metadata_file[FILE_YEAR_LABEL], metadata_file)
                except ValueError as e:
                    print(
                        f"Não foi possível transformar o arquivo {metadata_file[FILE_NAME_LABEL]} em um dataframe: {e}")
                    continue

                df_analysis = creater_analysis.create_schema_analysis(df)
                df_validator = read_validator(metadata_file)

                validatordata = schema_validator.SchemaValidator(
                    df_analysis, df_validator)

                validate_results.append(
                    (validatordata.check_if_dataframe_is_empty()))

                validate_results.append(
                    (validatordata.validate_number_fields()))

                validate_results.append(
                    (validatordata.validate_matched_columns("column_name")))

                validate_results.append(
                    (validatordata.check_unique_primary_key()))
                
                validate_results.append(
                    (validatordata.validate_data_types()))
                
                validate_results.append(
                    (validatordata.check_for_missing_values()))

                errors_filtered = [
                    error_filtered for error_filtered in validate_results if error_filtered != None]

                if len(errors_filtered) == 0 or metadata_file[SCHEMA_LABEL] == 'PlanAttributes':
                    validate_input_files.append(metadata_file)
                else:
                    errors_filtered = [(error,
                                        metadata_file[FILE_ID_LABEL], metadata_file[FILE_NAME_LABEL], metadata_file[SCHEMA_LABEL]) for error in errors_filtered]
                    errors_filtered_list.extend(errors_filtered)

            ti = kwargs['ti']
            ti.xcom_push(key='schema_error_files', value=errors_filtered_list)

            return validate_input_files

        validate_input_md_files = validator_schema(correct_input_md_files)

    with TaskGroup('ingest_data') as ingest_data:
        def rename_columns(df, df_validator, type):
            rename_dict = dict(
                zip(df_validator['column_name'], df_validator['column_name_formatted']))
            df = df.rename(columns=rename_dict)
            df = df.add_prefix(type.upper() + '_')

            return df

        @task()
        def read_and_handle_fields(validate_input_files, _output_format, **kwargs):
            errors_processed_list = []
            processed_md = []
            for md_file in validate_input_files:
                print('>>>>>>>>> Schema label in handle fields: ' +
                      md_file[SCHEMA_LABEL])

                try:
                    df = read_input(client, RAW_PATH + md_file[FILE_YEAR_LABEL], md_file)
                    df_validator = read_validator(md_file)
                    df = rename_columns(
                        df, df_validator, md_file[SCHEMA_LABEL])
                    
                    #anonimization secrety columns
                    filtered_columns = df_validator.loc[df_validator['is_anonymizated_field'], 'column_name'].values
                    df = anonymization.encode_columns(df, filtered_columns)

                    df = handle_columns.handle_df(df, md_file[SCHEMA_LABEL])
                except ValueError as e:
                    error = str(e)
                    if "Error na leitura do arquivo" in error:
                        error = f"Error na leitura do arquivo: {e}"
                    elif "Error no rename das colunas" in error:
                        error = f"Error no rename das colunas: {e}"
                    elif "Error no tratamento das colunas" in error:
                        error = f"Error no tratamento das colunas: {e}"
                    else:
                        error = f"Erro na escrita no arquivo temporario: {e}"
                    print(error)
                    errors_processed_list.append(error)

                try:
                    date = datetime.fromtimestamp(int(dtime))
                    formated_date = date.strftime("%Y_%m_%d")
                    md_file[VALIDATION_DATE_LABEL] = str(dtime)
                    md_file[PDSA_UPDATE_DATE_LABEL] = str(dtime)
                    md_file[PROCESSED_DATA_FORMAT_LABEL] = _output_format
                    md_file[NEW_FILE_NAME_LABEL] = md_file[SCHEMA_LABEL] + '_' + md_file[FILE_YEAR_LABEL] + '.' + _output_format

                except ValueError as e:
                    error = f"Error na escrita dos metadados: {e}"
                    print(error)
                    errors_processed_list.append(error)

                try:
                    print('>>>>>>>>> Save File: ' +
                          md_file[SCHEMA_LABEL])
                    print(df.head(2))

                    client.upload_data_frame(df, PDS_TMP_PATH, md_file[NEW_FILE_NAME_LABEL],
                                             remote_output_format=_output_format,
                                             file_metadata=md_file,
                                             overwrite_flag=True)
                    processed_md.append(md_file)
                except ValueError as e:
                    error = f"Error ao salvar o arquivo nos dados temporários: {e}"
                    print(error)
                    errors_processed_list.append(error)

            ti = kwargs['ti']
            ti.xcom_push(key='errors_processed_list',
                         value=errors_processed_list)
            return processed_md

        processed_md = read_and_handle_fields(
            validate_input_md_files, 'parquet')

    with TaskGroup('load_data') as load_data:
        @task()
        def load_to_processed_data(client, md_list):
            data_processed_list = []
            for md in md_list:
                filename = md[NEW_FILE_NAME_LABEL]

                _data, _, _metadata = client.get_file(
                    PDS_TMP_PATH, filename)

                metadata = _metadata['metadata']

                data_path = PDS_CLEAN_PATH + md[SCHEMA_LABEL]

                client.upload_file(data_path, filename,
                                   _data,
                                   file_metadata=metadata,
                                   overwrite_flag=True)
                data_processed_list.append(filename)

            return data_processed_list

        data_processed_results = load_to_processed_data(client, processed_md)

    with TaskGroup('clean_ingest') as clean_ingest:
        @task()
        def delete_tmp_processed_data(client, metadado_list):
            for md in metadado_list:
                client.rm_file(PDS_TMP_PATH, md[NEW_FILE_NAME_LABEL])

        delete_tmp_processed_data(client, processed_md)


    with TaskGroup('check_new_files') as check_new_files:

        @task.branch()
        def is_there_new_files(new_files_list):
            if (len(new_files_list) != 0):
                branch_result = 'check_new_files.continue_pipeline'
            else:
                branch_result = 'check_new_files.stop_pipeline'
            return branch_result

        @task()
        def continue_pipeline():
            TriggerDagRunOperator(
                task_id='trigger_dataset_curation',
                trigger_dag_id=default_args['next_dag'],
                execution_date='{{ ds }}',
                reset_dag_run=True,
                # wait_for_completion=True,
                # allowed_states=[State.SUCCESS, State.FAILED],
                # failed_states="None" # None doesn't work now
            )

        @task()
        def stop_pipeline():
            raise AirflowFailException("There are't new files")

        branch = is_there_new_files(data_processed_results)
        branch >> Label("There are new files") >> continue_pipeline()
        branch >> Label("There aren't new files") >> stop_pipeline() >> end

start >> Config >> list_inputs >> validate_schema >> ingest_data >> load_data >> [
    clean_ingest, check_new_files] >> end
