from .main_load import LoadOmoppedData
import logging
from typing import Optional
import pandas as pd
import numpy as np
import rpy2.robjects as ro
from rpy2.robjects.packages import importr
from rpy2.robjects import pandas2ri

# Configure logging
logging.basicConfig(level=logging.DEBUG)  # Use DEBUG level for detailed logging

class LoadPerson(LoadOmoppedData):
    def load_data(self):
        """Load Person data into the OMOP Person table."""
        try:
            # Retrieve existing person_source_value records
            queried_data_pandas = self.retrieve_persons()
            # Initialize an empty set and update with existing values
            existing_values = set(queried_data_pandas['person_source_value'])

            # Filter the new data to only include unique person_source_value entries
            filtered_data = self._omopped_data[
                ~self._omopped_data['person_source_value'].isin(existing_values)
            ]

            if filtered_data.empty:
                logging.info("No new data to insert for person; all records already exist in the target table.")
                return

            # Convert the filtered pandas DataFrame back to an R DataFrame
            filtered_data_r = self.convert_dataframe(filtered_data, direction='py_to_r')

            # Insert the filtered data into the target table
            self._db_connector.insertTable(
                connection=self._conn,
                tableName=f'{self._schema}.{self._table}',
                data=filtered_data_r,
                dropTableIfExists=False,
                createTable=False,
                tempTable=False,
                progressBar=True,  # You can turn on/off the progress bar based on your need
                useMppBulkLoad=False  # Avoid complex loading mechanisms during debugging
            )
            logging.info(f"Loaded data into table '{self._schema}.{self._table}'.")

        except Exception as e:
            logging.error(f"Failed to load data into table: {e}")

