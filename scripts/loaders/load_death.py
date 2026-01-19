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

class LoadDeath(LoadOmoppedData):
    def load_data(self):
        """Load death data into death table."""
        try:
            query_utils = QueryUtils(self._conn, self._schema, self._table, self.get_csv_loader(), self._vocab_schema)
            # retrieve person records
            queried_person = query_utils.retrieve_persons()
            # join both tables using inner join.
            self._omopped_data = self._omopped_data.merge(queried_person, on='person_source_value', how='inner')
            # get all death records.
            queried_deaths = query_utils.retrieve_death()
            # get unique deaths
            existing_deaths = set(queried_deaths['person_id'])
            # drop columns that are not needed
            self._omopped_data.drop(columns=['person_source_value'], inplace=True)            
            # Filter the new data to only include unique person_id entries            
            filtered_data = self._omopped_data[
                ~self._omopped_data['person_id'].isin(existing_deaths)
            ]
            # check if there are new records to insert
            if filtered_data.empty:
                logging.info("No new data to insert for death; all records already exist in the target table.")
                return
            
            # avoid duplicates
            filtered_data = filtered_data.drop_duplicates(subset=['person_id'], keep='first')
            # push the filtered data to the database
            asyncio.run(self.push_to_db(
                batch_size=250000,
                data=filtered_data,
                table_name=self._table
            ))            
            logging.info(f"Loaded data into table '{self._schema}.{self._table}'.")

        except Exception as e:
            logging.error(f"Failed to load data into table: {e}")