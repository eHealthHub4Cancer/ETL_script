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
from itertools import islice

# Configure logging
logging.basicConfig(level=logging.DEBUG)  # Use DEBUG level for detailed logging

class LoadVisitDetails(LoadOmoppedData):
    def load_data(self):
        """Load encounter data into the OMOP visit details table."""
        try:
            query_utils = QueryUtils(self._conn, self._schema, self._table, self.get_csv_loader())
            # retrieve person records
            queried_person = query_utils.retrieve_persons()
            # join both tables using inner join.
            self._omopped_data = self._omopped_data.merge(queried_person, on='person_source_value', how='inner')
            # retrieve past visit details
            queried_details = query_utils.retrieve_visit_details()
            # get unique visit details.
            existing_details = set(queried_details['visit_detail_id'])
            # get existing visits
            filtered_data = self._omopped_data[
                ~self._omopped_data['visit_detail_id'].isin(existing_details)
            ]
            # check if there are new records to insert
            if filtered_data.empty:
                logging.info("No new data to insert for visit occurrence; all records already exist in the target table.")
                return
            
            queried_visits = query_utils.retrieve_visits()
            sorted_data = filtered_data.sort_values(by='visit_source_value')
            # get the unique visit source values
            sorted_visits = queried_visits.sort_values(by='visit_source_value')
            # merge
            filtered_data = sorted_data.merge(sorted_visits, on='visit_source_value', how='inner')
            # convert the visit detail source concept id to string
            filtered_data['visit_detail_concept_id'] = filtered_data['visit_detail_concept_id'].astype(str)
            # get unique codes
            unique_code = filtered_data['visit_detail_concept_id'].unique().tolist()
            # get the concept id
            unique_concept_id = query_utils.retrieve_concept_id(code=unique_code, vocabulary=('SNOMED'))
            # # merge the concept id
            filtered_data['visit_detail_concept_id'] = filtered_data['visit_detail_concept_id'].map(unique_concept_id).astype(int)
            # get all care sites
            queried_care_sites = query_utils.retrieve_care_sites()
            # merge based on care site
            filtered_data = filtered_data.merge(queried_care_sites, on='care_site_source_value', how='left')
            filtered_data['admitted_from_concept_id'] = filtered_data['admitted_from_concept_id'].astype(str)
            # replace nan with 0
            filtered_data['admitted_from_concept_id'] = filtered_data['admitted_from_concept_id'].replace('nan', '0')
            filtered_data['admitted_from_concept_id'] = filtered_data['admitted_from_concept_id'].apply(lambda x: str(int(float(x))) if x.endswith('.0') else x)
            # get the unique codes
            unique_code = filtered_data['admitted_from_concept_id'].unique().tolist()
            # # get the concept id
            unique_concept_id = query_utils.retrieve_concept_id(code=unique_code, vocabulary=('SNOMED'))
            # merge the concept id
            filtered_data['admitted_from_concept_id'] = filtered_data['admitted_from_concept_id'].map(unique_concept_id).astype(int)
            # get all providers
            queried_providers = query_utils.retrieve_providers()
            # merge based on provider
            filtered_data = filtered_data.merge(queried_providers, on='provider_source_value', how='left')
            # drop columns that are not needed 
            filtered_data.drop(columns=['person_source_value', 'visit_source_value', 'provider_source_value', 'care_site_source_value'], inplace=True)            
            # only keep the columns that are not duplicates
            filtered_data = filtered_data.drop_duplicates(subset=['visit_detail_id'], keep='first')
            # # strip the length for admitted from source value.
            filtered_data['admitted_from_source_value'] = filtered_data['admitted_from_source_value'].apply(query_utils.strip_length)
            # push the filtered data to the database
            asyncio.run(self.push_to_db(
                batch_size=250000,
                data=filtered_data,
                table_name=self._table
            ))            
            logging.info(f"Loaded data into table '{self._schema}.{self._table}'.")

        except Exception as e:
            logging.error(f"Failed to load data into table: {e}")