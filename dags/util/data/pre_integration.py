import datetime


def get_latest_file(client, dir_path, type_file, valid_extensions, date_param_get):
    path = dir_path + '/' + type_file + '/'
    path_file_list = client.list_files(path)

    latest_date = None
    latest_file = None
    list_file_errors = []

    for path_file in path_file_list:
        file_name = path_file.split("/")[-1]

        try:
            if any(file_name.endswith(ext) for ext in valid_extensions):
                _, _, file_metadata = client.get_file(path, file_name)
                file_md = file_metadata.metadata

                file_time = file_md[date_param_get]

                try:
                    data_obj = datetime.datetime.strptime(
                        file_time, '%Y-%m-%d')
                    file_time = int(data_obj.timestamp())
                    print(file_time)
                except ValueError:
                    print(
                        f"A data '{file_time}' não está no formato esperado (YYYY-MM-DD).")

                if latest_date is None or int(file_time) >= int(latest_date):
                    latest_date = file_time
                    latest_file = path_file
        except Exception as e:
            error = f"Erro de leitura do arquivo processado {file_name}: {e}"
            print(error)
            list_file_errors.append(error)

    latest_date_understandable = datetime.datetime.fromtimestamp(
        int(latest_date))
    date_time_str = latest_date_understandable.strftime("%Y-%m-%d %H:%M:%S")

    return [latest_file, date_time_str, list_file_errors]


def get_latest_types(client, dir_path, type_file_list, valid_extensions, date_param_get):
    latest_time_files = []
    latest_files_error = []
    for type_file in type_file_list:
        latest_file = get_latest_file(
            client, dir_path, type_file, valid_extensions, date_param_get)

        latest_file.insert(0, type_file)
        latest_time_files.append(latest_file[:-1])
        latest_files_error.append(latest_file[-1])

    return latest_time_files, latest_files_error
