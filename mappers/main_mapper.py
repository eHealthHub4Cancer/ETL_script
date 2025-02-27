import os
import time
import pandas as pd
from dotenv import load_dotenv
import sys

# move to the root directory
# sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from scripts.loaders.connector import ConnectToDatabase

class BaseETLPipeline:
    def __init__(self):
        load_dotenv()
        self.db_config = {
            "port": os.getenv("DB_PORT"),
            "dbms": os.getenv("DB_TYPE"),
            "server": os.getenv("DB_SERVER"),
            "database": os.getenv("DB_NAME"),
            "password": os.getenv("DB_PASSWORD"),
            "user": os.getenv("DB_USER"),
            "driver_path": os.getenv("DRIVER_PATH"),
            "db_schema": os.getenv("DB_SCHEMA"),
        }
        self.file_path = os.getenv("FILE_PATH")
        self.db_connector = ConnectToDatabase(**self.db_config)
    
    def process_file(self, file, file_name, etl_mapping, custom: bool = False):
        if file in etl_mapping:
            get_file = file
            file = file.rsplit("_", 1)[0]
            etl_class, loader_class, fields = etl_mapping[get_file]

            
            file_path = os.path.join(self.file_path, file_name[0])
            print(f"Loading {file} data...")
            if custom:
                etl_instance = etl_class(file_path=file_path, table_name=file, fields_map=fields)
            else:
                etl_instance = etl_class(file_path=file_path, table_name=file, fields_map=fields)
            etl_instance.run_mapping(fields=fields)
            load_result = loader_class(self.db_connector, etl_instance.get_omopped_data(), file)
            load_result.load_data()
            time.sleep(1)
            print("\n\n")
        else:
            print(f"Skipping {file}, no ETL mapping found.")

    def run(self, etl_mapping, files_to_map, custom: bool = False):
        print("Connecting to database...")
        for file, file_name in files_to_map.items():
            self.process_file(file, file_name, etl_mapping, custom)
        print("ETL Pipeline Execution Completed.")