from datetime import datetime
import logging

# airflow
from airflow.exceptions import AirflowFailException
from airflow.models import Variable


class BasicTracker:

    def __init__(self, variable_name='basic_tracking'):
        """
        Abstract Tracking Mechanims

        Parameters
        ----------
        variable_name : str
            Airflow variable Name

        Returns
        -------
        new Tracking client

        """
        self.tracking_variable = variable_name
    
    def set_tracking_variable(self,value):
        """
            PRIVATE METHOD

            Overwrite the Ingestion tracking variable

            Parameters
            ----------
            value  : obj (json or dict)
        """
        Variable.set(key=self.tracking_variable, value=value, serialize_json=True)

    def __get_tracking_variables(self):
        """
            PRIVATE METHOD

            Retriver the Ingestion tracking variable
            
            Returns
            ----------
            value  : (dict) Airflow variable
        """
        var = {}

        try:
            var = Variable.get(key=self.tracking_variable, deserialize_json=True)
            # var = Variable.get(key=_key, deserialize_json=False)
        except KeyError:
            logging.debug("EMPTY VAR")
        
        return var
    
    def get_tracking(self):
        """
            Retriver the Ingestion tracking variable
            
            Returns
            ----------
            value  : (dict) Airflow variable
                     - empty dict if variable is unset
        """
        return self.__get_tracking_variables()
    

class IngestionTracker(BasicTracker):
    def __init__(self, ingestion_variable_name='aa_ingestion_file_list'):
        """
        Initialize Ingestion Tracking Mechanims

        Parameters
        ----------
        ingestion_variable_name : str
            Airflow variable Name

        Returns
        -------
        new Tracking client

        """
        super().__init__(variable_name=ingestion_variable_name)

    def is_file_processed(self,hash):
        """
            Check if file fash is already processed

            Parameters
            ----------
                hash  : file hash (string see backend def)
            
            Returns
            ----------
                - True if file is processed
                - False if file isnt processed
        """
        i_dict = self.get_tracking()
        try:
            i_dict[hash]
        except KeyError:
            return False

        return True
    
    def add_file(self,metadata):
        """
            Add a file to the ingested file lists

            Parameters
            ----------
                file metadata
            
            Raises
            ----------
                - AirflowFailException if tries to add a file tha is aready added 
                
        """
        # ivar = self.__get_tracking_variables()
        ivar = self.get_tracking()
        l_ivar = len(ivar)

        file_id = metadata['id'] #TODO verificar se e id ou ID
        file_hash = metadata['content_hash']
        file_name = metadata['filename']

        try:
            ivar[file_hash]
            raise AirflowFailException("File Already in Ingestion List")
        except KeyError:
            dtime = datetime.now().timestamp()
            input = {'file_id' : file_id,
                     'file_name' : file_name,
                     'ingestion_timestamp' : str(dtime)
                     }
            ivar[file_hash] = input
            self.set_tracking_variable(ivar)

class MergeTracker(BasicTracker):

    def __init__(self, ingestion_variable_name='aa_track_merge'):
        """
        Initialize Merge Tracking Mechanims

        Parameters
        ----------
        ingestion_variable_name : str
            Airflow variable Name

        Returns
        -------
        new Tracking client

        """
        super().__init__(variable_name=ingestion_variable_name)
    
    def update_merge_tracking(self,merge_name,file_hash_list):
        """
        Track the files (the content_hash of the files) to generate a merge (e.g., file.parquet)

        Parameters
        ----------
            - merge_name (str) : merge name (e.g., file.parquet)
            - file_hash_list ( list of str) : list of hash of the files used to generate the merge
        """
        af_var = self.get_tracking()
        l_ivar = len(af_var)

        af_var[merge_name] = file_hash_list
        self.set_tracking_variable(af_var)
    
    def is_mergeble(self,merge_name,file_hash_list):
        """
        Check wheter the files (the content_hash of the files) is new to  generate a merge (e.g., file.parquet)

        It only perform a merge if all files are new

        Parameters
        ----------
            - merge_name (str) : merge name (e.g., file.parquet)
            - file_hash_list ( list of str) : list of hash of the files used to generate the merge
        """
        af_var = self.get_tracking()
        return_flag = False

        try:
            var_file_list = af_var[merge_name]

            if len(var_file_list) != len(file_hash_list):
                # raise AirflowFailException("File Already in Ingestion List")
                return_flag = False
            
            count = 0
            for i in file_hash_list:
                # logging.info("debug ::: ")
                # logging.info("\t" + i)
                if not i in var_file_list:
                    count += 1
            
            # so retorna se todos os arquivos forem novos
            if count == len(var_file_list):
                return_flag = True
            
        except KeyError:
            return_flag = True # caso nao existe o track e possivel gerar o merge
            logging.info("KEY ERROR")
            logging.info("\t merge_name {}".format(merge_name))
            logging.info("\t file_hash_list {}".format(file_hash_list))
            logging.info("")
            logging.info("\t tracking {}".format(af_var))
            logging.info("===================================================================")
        
        return return_flag