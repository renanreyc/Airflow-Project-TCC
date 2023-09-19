import pandas as pd
from io import BytesIO

from datetime import datetime, timedelta


def purge_older_files(client, path, info_data_metadata_label, file_day_deadline):
    list_errors = []
    list_purge_files = []
    valid_extensions = ['parquet', 'csv', 'xlsx', 'xlsb']
    now_date = datetime.now().date()
    limit_date = now_date - timedelta(days=file_day_deadline)

    remote_files = client.list_files(path)

    for path_full_name in remote_files:
        file_name = path_full_name.split("/")[-1]
        try:
            if any(file_name.endswith(ext) for ext in valid_extensions):
                _, _, file_metadata = client.get_file(path, file_name)

                file_md = file_metadata.metadata
                date_file_formated = datetime.strptime(
                    file_md[info_data_metadata_label], '%Y-%m-%d').date()

                if limit_date > date_file_formated:
                    client.rm_file(path, file_name)
                    print(
                        f'arquivo excluido: {file_name} com data {file_md[info_data_metadata_label]}')
                    list_purge_files.append(path + file_name)
        except Exception as e:
            error = f"Erro de leitura dos metadados ao tentar deletar o arquivo {file_name}: {e}"
            print(error)
            list_errors.append(error)

    return list_purge_files
