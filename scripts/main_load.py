from abc import ABC, abstractmethod
from ohdsi_cdm_loader.db_connector import DatabaseHandler
import logging
from rpy2.robjects import pandas2ri
from rpy2 import robjects as ro
from rpy2.robjects.packages import importr
from typing import Optional

# Configure logging
logging.basicConfig(level=logging.DEBUG)  # Use DEBUG level for detailed logging

class LoadOmoppedData(ABC):
    def __init__(self, dbms: str, server: str, user: str, password: str, database: str, driver_path: str, db_schema: str, omop_data: object, omop_table: str, port: int = 5432):
        """
        Initialize the DatabaseHandler with the given parameters.

        :param dbms: The database management system type.
        :param server: The server address.
        :param user: The username for database authentication.
        :param password: The password for database authentication.
        :param database: The name of the database.
        :param driver_path: Path to the database driver.
        :param port: This defines the port of the database.
        :param db_schema: The schema to be used in the database.
        :param omop_data: The OMOP data to be loaded.
        :param omop_table: The target OMOP table name.
        """
        self._dbms = dbms
        self._server = server
        self._user = user
        self._password = password
        self._database = database
        self._driver_path = driver_path
        self._conn: Optional[object] = None
        self._conn_details: Optional[object] = None
        self._port = port
        self._schema = db_schema
        self._omopped_data = omop_data
        self._table = omop_table
        self._db_connector = importr('DatabaseConnector')
        self.create_connection()

    def create_connection(self):
        """Create a connection to the database."""
        self._conn_details = DatabaseHandler(
            self._dbms, self._server,
            self._user, self._password,
            self._database, self._driver_path, self._port
        )
        self._conn = self._conn_details.connect_to_db()

    def convert_dataframe(self, data, direction='r_to_py'):
        """
        Converts a DataFrame between R and pandas.

        Parameters:
        - data: The DataFrame to convert.
        - direction: str, optional, either 'r_to_py' or 'py_to_r'.
                     'r_to_py' for R to pandas conversion (default).
                     'py_to_r' for pandas to R conversion.

        Returns:
        - Converted DataFrame.
        """
        with (ro.default_converter + pandas2ri.converter).context():
            conversion = ro.conversion.get_conversion()
            
            if direction == 'r_to_py':
                # Convert R DataFrame to pandas DataFrame
                logging.debug("Converting R DataFrame to pandas DataFrame.")
                return conversion.rpy2py(data)
            elif direction == 'py_to_r':
                # Convert pandas DataFrame to R DataFrame
                logging.debug("Converting pandas DataFrame to R DataFrame.")
                return conversion.py2rpy(data)
            else:
                raise ValueError("Invalid direction. Use 'r_to_py' or 'py_to_r'.")

    @abstractmethod
    def load_data(self):
        """
        Abstract method to load data into the database.
        This method should be implemented by subclasses to handle specific OMOP tables.
        """
        pass