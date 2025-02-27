from ohdsi_cdm_loader.db_connector import DatabaseHandler
from ohdsi_cdm_loader.load_csv import CSVLoader
import logging
from rpy2.robjects import pandas2ri
from rpy2 import robjects as ro
from rpy2.robjects.packages import importr
from typing import Optional

# Configure logging
logging.basicConfig(level=logging.DEBUG)  # Use DEBUG level for detailed logging

class ConnectToDatabase:
    def __init__(self, dbms: str, server: str, user: str, password: str, database: str, driver_path: str, db_schema: str, port: int = 5432):
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
        self._db_connector = importr('DatabaseConnector')
        self.create_connection()

    def create_connection(self):
        """Create a connection to the database."""
        self._conn_details = DatabaseHandler(
            self._dbms, self._server,
            self._user, self._password,
            self._database, self._driver_path, 
            self._schema,self._port
        )
        self._conn = self._conn_details.connect_to_db()
        self._db_loader = CSVLoader(self._conn, self._conn_details)

        
