import pandas as pd
from io import BytesIO

from datetime import datetime, timedelta


def switch_data(data, name_file, metadata_type, sheet=None):
    try:
        df = pd.read_csv(BytesIO(data))
        return df

    except Exception as e:
        print(f"error ao transformar o arquivo {name_file} em dataframe: {e}")

def specified_file(client, path, schema_names, extensions):
    valid_extensions = extensions
    specified_files = []
    list_errors = []

    remote_files = client.list_files(path)

    for file in remote_files:

        file_name_split = file.split("/")
        file_name = file_name_split[-1]
        if(len(file_name_split) > 2):
            sub_path = file_name_split[-2]
        else:
            sub_path = ''      

        try:
            if '.' in file_name:
                if any(file_name.endswith(ext) for ext in valid_extensions):
                    _, _, file_metadata = client.get_file(path + sub_path, file_name)
                    file_md = file_metadata.metadata

                    if file_name != file_md['filename']:
                        error = f"O nome do arquivo {file_name} é diferente do filename metadata: {file_md['filename']}"
                        print(error)
                        list_errors.append(error)
                        continue

                    if file_md['schema'] in schema_names:
                        specified_files.append(
                            file_md)
                    else:
                        error = f"O arquivo {file_name} não tem um dos schemas aceitos: {schema_names}"
                        print(error)
                        list_errors.append(error)
                else:
                    error = f"O arquivo {file_name} não tem uma das extensões aceitas: {extensions}"
                    print(error)
                    list_errors.append(error)

        except Exception as e:
            error = f"Erro de leitura dos metadados ao processar o arquivo {file_name}: {e}"
            print(error)
            list_errors.append(error)

    return specified_files, list_errors
