import os
import sys
from zipfile import ZipFile
from hate.logger import logging
from hate.exception import CustomException
from hate.configuration.gcloud_syncer import GCloudSync
from hate.entity.config_entity import DataIngestionConfig
from hate.entity.artifact_entity import DataIngestionArtifacts

class DataIngestion:
    def __init__(self,data_ingestion_config:DataIngestionConfig):
        self.data_ingestion_config=data_ingestion_config
        self.gcloud=GCloudSync()

    def get_data_from_gcloud(self)->None:
        try:
            logging.info("Entered the gcloud function")
            self.gcloud.sync_folder_from_gcloud(self.data_ingestion_config.BUCKET_NAME,
                                                self.data_ingestion_config.ZIP_FILE_NAME,
                                                self.data_ingestion_config.DATA_INGESTION_ARTIFACTS_DIR,)
            logging.info("Exited")
        except Exception as e:
            raise CustomException(e,sys) from e
    def unzip_and_clean(self):
        logging.info("Entered the zip thing")
        try:
            with ZipFile(self.data_ingestion_config.ZIP_FILE_PATH,'r') as ref:
                ref.extractall(self.data_ingestion_config.ZIP_FILE_DIR)
            logging.info("Exited zip")
            return self.data_ingestion_config.DATA_ARTIFACTS_DIR,self.data_ingestion_config.NEW_DATA_ARTIFACTS_DIR
        
        except Exception as e:
            raise CustomException(e,sys) from e
    def initiate_data_ingestion(self):
        logging.info("Entered intiating")
        try:
            self.get_data_from_gcloud()
            logging.info("Fetched data from s3")
            imbalance_data_file_path,raw_data_file_path=self.unzip_and_clean()
            logging.info("unzipping done successfully")
            data_ingestion_artifacts=DataIngestionArtifacts(
                imbalance_data_file_path=imbalance_data_file_path,
                raw_data_file_path=raw_data_file_path
            )
            return data_ingestion_artifacts
        except Exception as e:
            raise CustomException(e,sys)
        
        
        

