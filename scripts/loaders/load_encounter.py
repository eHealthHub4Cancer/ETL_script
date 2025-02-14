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

class LoadEncounter(LoadOmoppedData):
    def load_data(self):
        """Load encounter data into the OMOP visit occurrence table."""
        try:
            query_utils = QueryUtils(self._conn, self._schema, self._table, self.get_csv_loader())
            # retrieve person records
            queried_person = query_utils.retrieve_persons()
            # join both tables using inner join.
            self._omopped_data = self._omopped_data.merge(queried_person, on='person_source_value', how='inner')
            # fill the missing values for start dates with the observation period start date
            # logic to be adjusted later in the future based on quality check.
            # retrieve past visits.
            queried_visits = query_utils.retrieve_visit_occurrences()
            # get unique visit ids.
            existing_visits = set(queried_visits['visit_occurrence_id'])             
            # drop columns that are not needed
            self._omopped_data.drop(columns=['person_source_value'], inplace=True)            
            # Filter the new data to only include unique person_id entries
            filtered_data = self._omopped_data[
                ~self._omopped_data['visit_occurrence_id'].isin(existing_visits)
            ]
            # get the providers
            queried_providers = query_utils.retrieve_providers()
            # merge the data
            filtered_data = filtered_data.merge(queried_providers, on='provider_source_value', how='inner')
            # drop columns that are not needed
            # merge with care sites
            queried_care_sites = query_utils.retrieve_care_sites()
            # merge the data
            filtered_data = filtered_data.merge(queried_care_sites, on='care_site_source_value', how='inner')
            filtered_data.drop(columns=['provider_source_value', 'care_site_source_value'], inplace=True)

            # check if there are new records to insert
            if filtered_data.empty:
                logging.info("No new data to insert for visit occurrence; all records already exist in the target table.")
                return
            # only keep the columns that are not duplicates
            filtered_data = filtered_data.drop_duplicates(subset=['visit_occurrence_id'], keep='first')
            # push the filtered data to the database
            asyncio.run(self.push_to_db(
                batch_size=250000,
                data=filtered_data,
                table_name=self._table
            ))            
            logging.info(f"Loaded data into table '{self._schema}.{self._table}'.")

        except Exception as e:
            logging.error(f"Failed to load data into table: {e}")