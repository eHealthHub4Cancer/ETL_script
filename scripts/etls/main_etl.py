import pandas as pd
import logging
import uuid
from snowflake import SnowflakeGenerator
from abc import ABC, abstractmethod
from typing import Optional

class ETLEntity(ABC):
    def __init__(self, file_path: str, fields_map: Optional[list] = None):
        """
        Initialise the AbstractEntity class.
        Args:
            file_path: str - This defines the file path.
            fields_map: list - This defines the fields to be mapped.
            omop_table: str - This defines the table we are mapping to.
        """
        self._path = file_path
        self._fields_map = fields_map if fields_map else []

        # Initialize data as DataFrames
        self._source_data = pd.DataFrame(columns=self._fields_map)
        self._omop_data = pd.DataFrame(columns=self._fields_map)

    def load_data(self):
        """Load the source data from the file path."""
        try:
            self._source_data = pd.read_csv(self._path).rename(columns=str.lower)
            logging.info(f"Data loaded successfully from {self._path}")
        except FileNotFoundError:
            logging.error(f"File not found: {self._path}")
        except pd.errors.ParserError:
            logging.error(f"Error parsing file: {self._path}")
        except Exception as e:
            logging.error(f"Unexpected error loading data: {e}")

    def set_fields(self, fields):
        """Set the fields."""
        self._fields_map = fields

    def get_fields(self):
        """Get the fields."""
        return self._fields_map

    def map_data_to_fields(self):
        """Extract only the required fields for OMOP mapping."""
        try:
            self._omop_data = self._source_data[self._fields_map]
            logging.info("Data mapped to specified fields successfully.")
        except KeyError as e:
            logging.error(f"Mapping failed. Missing fields: {e}")

    def get_omopped_data(self):
        """Get the OMOP mapped data."""
        return self._omop_data

    @abstractmethod
    def map_data(self):
        """Abstract method to map specific fields to OMOP format."""
        pass

    def run_mapping(self, fields):
        """Run the complete mapping process."""
        self.load_data()
        self.set_fields(fields=fields)
        self.map_data()
        self.map_data_to_fields()
