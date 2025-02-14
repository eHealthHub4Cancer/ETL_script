from abc import ABC, abstractmethod
from ohdsi_cdm_loader.db_connector import DatabaseHandler
from ohdsi_cdm_loader.load_csv import CSVLoader
import logging
from rpy2.robjects import pandas2ri
from rpy2 import robjects as ro
from rpy2.robjects.packages import importr
from typing import Optional

# Configure logging
logging.basicConfig(level=logging.DEBUG)  # Use DEBUG level for detailed logging

class LoadOmoppedData(ABC):
    def __init__(self, connector: object, omop_data: object, omop_table: str):
        """
        Initialize the DatabaseHandler with the given parameters.
        :param omop_data: The OMOP data to be loaded.
        :param omop_table: The target OMOP table name.
        """
        self._conn = connector._conn
        self._conn_details = connector._conn_details
        self._schema = connector._schema
        self._omopped_data = omop_data
        self._table = omop_table
        self._db_connector = importr('DatabaseConnector')
        self._filtered_data: Optional[object] = None
        self._db_loader = connector._db_loader
    
    def get_csv_loader(self):
        """get the CSVLoader object."""
        return self._db_loader

    @abstractmethod
    def load_data(self):
        """
        Abstract method to load data into the database.
        This method should be implemented by subclasses to handle specific OMOP tables.
        """
        pass    
    
    async def push_to_db(self, batch_size, data, table_name):
        """Push data to the database."""
        try:
            await self._db_loader.bulk_load_data(
                batch_size=batch_size,
                data=data,
                table_name=table_name
            )
        except Exception as e:
            logging.error(f"Failed to load data into table: {e}")
        return