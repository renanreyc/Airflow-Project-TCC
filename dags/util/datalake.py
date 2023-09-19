import os
import sys
import fsspec
from datetime import datetime
from io import StringIO , BytesIO
from azure.storage.filedatalake import DataLakeServiceClient


class DataLake():
    
    def __init__(self, storage_account_name, storage_account_key, file_system_name, env_flag='prod'):
        """
        Initialize azure storage account

        client = initialize_storage_account(account,password)

        Parameters
        ----------
        storage_account_name : str
            Account name
        storage_account_key : str
            Account key
        
        file_system_name : str
            File System Name (eg., "test_data_lake")
        
        env_flag: str
            if env_flah == 'prod' than use https else use http (lucas hack)

        Returns
        -------
        new DataLake client

        Atributes
        ------
            - fs_name: str
            - service_client: DataLakeServiceClient
            - fs : DataLakeFileSystem

        """
        if env_flag == 'prod':
            account_url="{}://{}.dfs.core.windows.net".format("https", storage_account_name)
        else:
            account_url="{}://{}.dfs.core.windows.net".format("http", storage_account_name)

        self.fs_name = file_system_name

        self.service_client = DataLakeServiceClient(account_url, credential=storage_account_key)
        self.fs = self.service_client.get_file_system_client(file_system_name) #filesystem
    
    def get_client(self):
        return self.service_client
    
    def get_fs(self):
        return self.fs

    
    def mkdir(self, remote_path):
        """
        make folder

        make sure to create the full path before create a sub directory

        Parameters
        ----------
        remote_path : str
            Remote path (e.g., "/raw_data/teste_folder)

        Returns
        -------
        content: 
        """

        fs = self.get_fs()
        return fs.create_directory(remote_path)
    
    def rm_file(self, remote_path, remote_file):
        """
        remove a file

        Parameters
        ----------
        remote_path : str
            Remote path (e.g., "/raw_data/teste_folder)
        remote_file : str
            File name (e.g., "SAS.csv")

        Returns
        -------
        data: object
            the data
        data_type: type
            type of data (e.g., bytes)
        file_properties: dict
            a dictonarie with several medata of the action
        """
        rdir = self.fs.get_directory_client(remote_path) #direcotry, podemos escrever metadata aquit
        
        file_client = rdir.get_file_client(remote_file) #

        file_properties = file_client.get_file_properties()
        return file_client.delete_file()

    # danger nao utilizar
    # def rm_dir(self, remote_path, remote_sub_dir):
    #     r = client.fs.get_directory_client(remote_path) #direcotry, podemos escrever metadata aquit
    #     r.delete_sub_directory("y")
    
    
    def list_files(self, remote_path):
        """
        Retrieve the files in a folder

        Parameters
        ----------
        remote_path : str
            Remote path (e.g., "/raw_data/teste_folder)

        Returns
        -------
        content: list
            a list of all files and direcotires name in the remote_path
        """
        paths = self.fs.get_paths(path=remote_path)

        content=[]

        for path in paths:
            content.append(path.name)

        return content

    def get_file(self, remote_path, remote_file):
        """
        Retrieve a file from the data later

        Parameters
        ----------
        remote_path : str
            Remote path (e.g., "/raw_data/teste_folder)
        remote_file : str
            File name (e.g., "SAS.csv")

        Returns
        -------
        data: object
            the data
        data_type: type
            type of data (e.g., bytes)
        file_properties: dict
            a dictonarie with several medata of the file
        """
        rdir = self.fs.get_directory_client(remote_path) #direcotry, podemos escrever metadata aquit
        
        file_client = rdir.get_file_client(remote_file) #
        file_properties = file_client.get_file_properties()
        download = file_client.download_file() #binary files
        data = download.readall()

        return data, type(data), file_properties

    def upload_file(self, remote_path,
                    remote_file,local_file_path,
                    file_metadata=None,local_file_type="rb",
                    overwrite_flag=True):
        """
        Upload a file to the data lake

        Parameters
        ----------
        remote_path : str
            Remote path (e.g., "/raw_data/teste_folder)
        remote_file : str
            File name (e.g., "SAS.csv")
        local_file_path : str
            File to upload path name (e.g., "/tmp/saida.csv")
        file_metadata: dict
            dictonary with file metada
            default_value = None
        local_file_type: 'rb' or 'r'
            mode of the file (rb=binary,r=text)
            default_value = "rb"

        Returns
        -------
        file_properties: dict
            a dictonarie with several medata of the file
        """

        rdir = self.fs.get_directory_client(remote_path) #direcotry, podemos escrever metadata aquit
        file_client = rdir.get_file_client(remote_file)
        original_properties = file_client.create_file()

        if type(local_file_path) == str:
            with open(local_file_path, local_file_type) as f:
                data = f.read()
                if file_metadata == None:
                    file_client.upload_data(data=data,overwrite=overwrite_flag)    
                else:
                    file_client.upload_data(data=data,overwrite=overwrite_flag,metadata=file_metadata)
        else:
            data = local_file_path
            if file_metadata == None:
                    file_client.upload_data(data=data,overwrite=overwrite_flag)    
            else:
                file_client.upload_data(data=data,overwrite=overwrite_flag,metadata=file_metadata)
        
        return file_client.get_file_properties()

    def upload_data_frame(self,data_frame,
                        remote_path,remote_file,
                        remote_output_format='csv',
                        file_metadata=None,
                        overwrite_flag=True,
                        parquet_engine='fastparquet'):
        """
        Upload a Pandas.DataFrame to the data lake

        Parameters
        ----------
        data_frame: pandas.DataFrame
            Pandas Dataframe to be stored Pandas
        remote_path : str
            Remote path (e.g., "/raw_data/teste_folder)
        remote_file : str
            File name (e.g., "SAS.csv")
        remote_output_format : str
            output format of the dataframe (csv or parquet)
            default_value = csv
        file_metadata: dict
            dictonary with file metada
            default_value = None
        parquet_engine: str
            engine to save parquet file (pyarrow or)

        Returns
        -------
        file_properties: dict
            a dictonarie with several medata of the file
        """
        assert( remote_output_format in ['csv','parquet'] )
        assert( parquet_engine in ['pyarrow','fastparquet'] )

        rdir = self.fs.get_directory_client(remote_path) #direcotry, podemos escrever metadata aquit
        file_client = rdir.get_file_client(remote_file)
        original_properties = file_client.create_file()
        
        _buffer = BytesIO()

        if ( remote_output_format == 'csv' ):
            _buffer = StringIO()
            data_frame.to_csv(_buffer)

        if ( remote_output_format == 'parquet' ):
            _buffer = BytesIO()
            # data_frame.to_parquet(_buffer,'pyarrow')
            if parquet_engine == 'pyarrow':
                data_frame.to_parquet(_buffer,parquet_engine)
            else:
                random_name = remote_path + remote_file + str(datetime.now())
                data_frame.to_parquet("memory://"+random_name+".parquet",parquet_engine)

                # df.to_parquet("memory://temp.parquet")
                with fsspec.open("memory://"+random_name+".parquet", "rb") as f:
                    _buffer = BytesIO(f.read())
        
        if file_metadata == None:
            file_client.upload_data(_buffer.getvalue(),overwrite=overwrite_flag)    
        else:
            file_client.upload_data(_buffer.getvalue(),overwrite=overwrite_flag,metadata=file_metadata)

        
        return file_client.get_file_properties()