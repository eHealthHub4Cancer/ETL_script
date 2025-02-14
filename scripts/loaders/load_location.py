from .main_load import LoadOmoppedData
import logging
from typing import Optional
import pandas as pd
import numpy as np
from rpy2.robjects.packages import importr
from .query_utils import QueryUtils
import asyncio

# Configure logging
logging.basicConfig(level=logging.DEBUG)  # Use DEBUG level for detailed logging

class LoadLocation(LoadOmoppedData):
    def load_data(self):
        """Load location data into the OMOP Location table."""
        try:
            # retrieve existing location records.
            query_utils = QueryUtils(self._conn, self._schema, self._table, self.get_csv_loader())
            # generate location values
            retrieved_locations = query_utils.retrieve_locations()
            # get only unique locations ids
            existing_values = set(retrieved_locations['location_id'])
            # filter the new data to only include unique location_id entries
            filtered_data = self._omopped_data[
                ~self._omopped_data['location_id'].isin(existing_values)
            ]
            if filtered_data.empty:
                logging.info("No new data to insert for location; all records already exist in the target table.")
                return
            # push the filtered data to the database
            # only keep the columns that are not duplicates
            filtered_data = filtered_data.drop_duplicates(subset=['location_id'], keep='first')
            asyncio.run(self.push_to_db(
                batch_size=250000,
                data=filtered_data,
                table_name=self._table
            ))
            
            logging.info(f"Loaded data into table '{self._schema}.{self._table}'.")
            logging.info(f"total number of records: {len(filtered_data)}")

        except Exception as e:
            logging.error(f"Failed to load data into table: {e}")