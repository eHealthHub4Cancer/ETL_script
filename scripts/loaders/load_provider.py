from .main_load import LoadOmoppedData
import logging
from typing import Optional
import pandas as pd
import numpy as np
import rpy2.robjects as ro
from rpy2.robjects.packages import importr
from rpy2.robjects import pandas2ri
from .query_utils import QueryUtils
import asyncio

# Configure logging
logging.basicConfig(level=logging.DEBUG)  # Use DEBUG level for detailed logging

class LoadProvider(LoadOmoppedData):
    def load_data(self):
        """Load provider data."""
        try:
            query_utils = QueryUtils(self._conn, self._schema, self._table, self.get_csv_loader())
            # retrieve past providers.
            queried_providers = query_utils.retrieve_providers()
            # retrieve existing care site records.
            existing_providers = set(queried_providers['provider_id'])
            queried_care_sites = query_utils.retrieve_care_sites()
            # merge the data
            self._omopped_data = self._omopped_data.merge(queried_care_sites, on='care_site_source_value', how='left')
            # drop columns that are not needed
            self._omopped_data.drop(columns=['care_site_source_value'], inplace=True)            
            # Filter the new data to only include unique provider_id entries
            
            filtered_data = self._omopped_data[
                ~self._omopped_data['provider_id'].isin(existing_providers)
            ]
            
            # check if there are new records to insert
            if filtered_data.empty:
                logging.info("No new data to insert for Providers; all records already exist in the target table.")
                return
            # only keep the columns that are not duplicates
            filtered_data = filtered_data.drop_duplicates(subset=['provider_id'], keep='first')
            # push the filtered data to the database
            asyncio.run(self.push_to_db(
                batch_size=250000,
                data=filtered_data,
                table_name=self._table
            ))            
            logging.info(f"Loaded data into table '{self._schema}.{self._table}'.")

        except Exception as e:
            logging.error(f"Failed to load data into table: {e}")