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

class LoadPerson(LoadOmoppedData):
    def load_data(self):
        """Load Person data into the OMOP Person table."""
        try:
            # Retrieve existing person_source_value records
            query_utils = QueryUtils(self._conn, self._schema, self._table, self.get_csv_loader(), self._vocab_schema)
            queried_data_pandas = query_utils.retrieve_persons()
            # Initialize an empty set and update with existing values
            existing_values = set(queried_data_pandas['person_source_value'])
            # Filter the new data to only include unique person_source_value entries
            filtered_data = self._omopped_data[
                ~self._omopped_data['person_source_value'].isin(existing_values)
            ]

            if filtered_data.empty:
                logging.info("No new data to insert for person; all records already exist in the target table.")
                return
            
            if 'location_id' not in filtered_data.columns or filtered_data['location_id'].isna().all():
                # get location ids
                locations = query_utils.retrieve_locations()
                # merge the data
                filtered_data = filtered_data.merge(
                    locations,
                    left_on='location_source_value',
                    right_on='location_source_value',
                    how='left',
                )
            if 'location_id_x' in filtered_data.columns or 'location_id_y' in filtered_data.columns:
                location_id = None
                if 'location_id_x' in filtered_data.columns:
                    location_id = filtered_data['location_id_x']
                if 'location_id_y' in filtered_data.columns:
                    location_id = (
                        location_id.fillna(filtered_data['location_id_y'])
                        if location_id is not None
                        else filtered_data['location_id_y']
                    )
                if location_id is not None:
                    filtered_data['location_id'] = location_id
                filtered_data.drop(columns=['location_id_x', 'location_id_y'], inplace=True, errors='ignore')
            # remove the location_source_value column
            filtered_data.drop(columns=['location_source_value'], inplace=True, errors='ignore')
            # drop duplicates.
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
