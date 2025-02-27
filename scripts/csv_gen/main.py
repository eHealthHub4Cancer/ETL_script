import os
import pandas as pd
from rpy2.robjects.packages import importr
import logging
import pyarrow.feather as feather

# Configure logging
logging.basicConfig(level=logging.DEBUG)  # Use DEBUG level for detailed logging


class CSVGen:
    def __init__(self, db_conn, table_names: list, save_dir: str, schema: str):
        """
        Initialize the CSVGen class.

        :param db_conn: Database connection object
        :param table_names: List of table names to export
        :param save_dir: Directory where CSV files will be saved
        """
        self._conn = db_conn
        self._db_connector = importr('DatabaseConnector')
        self._arrow = importr('arrow')
        self._table_names = table_names
        self._save_dir = save_dir
        self._schema = schema
        # Ensure the save directory exists
        os.makedirs(self._save_dir, exist_ok=True)

    def convert_dataframe(self, data, direction='r_to_py'):
        """
        Converts a DataFrame between R and pandas.
        source: https://rpy2.github.io/doc/latest/html/generated_rst/pandas.html
        
        Parameters:
        - data: The DataFrame to convert.
        - direction: str, optional, either 'r_to_py' or 'py_to_r'.
                     'r_to_py' for R to pandas conversion (default).
                     'py_to_r' for pandas to R conversion.

        Returns:
        - Converted DataFrame.
        """
        # with (ro.default_converter + pandas2ri.converter).context():
        #     conversion = ro.conversion.get_conversion()
            
        if direction == 'r_to_py':
            # Convert R DataFrame to pandas DataFrame
            logging.debug("Converting R DataFrame to pandas DataFrame.")
            self._arrow.write_feather(data, 'rdf.feather')
            return pd.read_feather('rdf.feather')
        
        elif direction == 'py_to_r':
            # Convert pandas DataFrame to R DataFrame
            logging.debug("Converting pandas DataFrame to R DataFrame.")
            feather.write_feather(data, 'pdf.feather')
            return self._arrow.read_feather('pdf.feather')
        else:
            raise ValueError("Invalid direction. Use 'r_to_py' or 'py_to_r'.")


    def generate_csv(self):
        """Generate CSV files for all tables in the list."""
        for table in self._table_names:
            # if empty skip
            if not table:
                continue
            self._generate_csv(table)
    
    def _generate_csv(self, table_name):
        """Generate a CSV file for a specific table."""
        query = f"""
        SELECT * FROM {self._schema}.{table_name}
        """
        # Read the data from the database
        data = self._db_connector.querySql(
            connection=self._conn,
            sql=query)
        
        queried_data = self.convert_dataframe(data, direction='r_to_py')

        # Save the CSV in the specified directory
        file_path = os.path.join(self._save_dir, f"{table_name}.csv")
        queried_data.to_csv(file_path, index=False)
        logging.info(f"✅ Saved: {file_path}")