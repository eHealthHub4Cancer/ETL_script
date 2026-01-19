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

class LoadCareSite(LoadOmoppedData):
    def load_data(self):
        """Load Care site data into the OMOP ObservationPeriod table."""
        try:
            # retrieve existing person_source_value records
            query_utils = QueryUtils(self._conn, self._schema, self._table, self.get_csv_loader(), self._vocab_schema)
            # retrieve location records
            queried_locations = query_utils.retrieve_locations()
            # merge the data
            self._omopped_data = self._omopped_data.merge(queried_locations, on='location_source_value', how='left')
            self._omopped_data.drop(columns=['location_source_value'], inplace=True)
            # Retrieve existing care site records
            queried_data_pandas = query_utils.retrieve_care_sites()
            # Initialize an empty set and update with existing values
            existing_values = set(queried_data_pandas['care_site_id'])
            # Filter the new data to only include unique person_id entries
            filtered_data = self._omopped_data[
                ~self._omopped_data['care_site_id'].isin(existing_values)
            ]
            if filtered_data.empty:
                logging.info("No new data to insert for care site; all records already exist in the target table.")
                return
            # only keep the columns that are not duplicates
            filtered_data = filtered_data.drop_duplicates(subset=['care_site_id'], keep='first')
            # push the filtered data to the database
            asyncio.run(self.push_to_db(
                batch_size=250000,
                data=filtered_data,
                table_name=self._table
            ))
            
            logging.info(f"Loaded data into table '{self._schema}.{self._table}'.")

        except Exception as e:
            logging.error(f"Failed to load data into table: {e}")