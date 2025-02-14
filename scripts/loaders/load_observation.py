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

class LoadObservation(LoadOmoppedData):
    def load_data(self):
        """Load observation"""
        try:
            query_utils = QueryUtils(self._conn, self._schema, self._table, self.get_csv_loader())
            # retrieve person records
            queried_person = query_utils.retrieve_persons()
            # join both tables using inner join.
            self._omopped_data = self._omopped_data.merge(queried_person, on='person_source_value', how='inner')
            # retrieve past observation records
            queried_observations = query_utils.retrieve_observations()
            # get unique observations.
            existing_observations = set(queried_observations['observation_id'])
            # query existing observations
            filtered_data = self._omopped_data[
                ~self._omopped_data['observation_id'].isin(existing_observations)
            ]
            # check if there are new records to insert
            if filtered_data.empty:
                logging.info("No new data to insert for observation; all records already exist in the target table.")
                return
            
            # retrieve visits
            queried_visits = query_utils.retrieve_visits()
            # merge
            filtered_data = filtered_data.merge(queried_visits, on='visit_source_value', how='inner')
            # retrieve concepts
            queried_concepts = query_utils.retrieve_concepts()
            # get only snomed vocabularies
            queried_concepts = queried_concepts[queried_concepts['vocabulary_id'].isin(['LOINC', 'SNOMED'])]
            # concept dict
            concept_dict = queried_concepts.set_index('concept_code')['concept_id'].to_dict()
            # merge based on concept code.
            filtered_data['observation_concept_id'] = filtered_data['observation_concept_id'].map(concept_dict)
            filtered_data['observation_type_concept_id'] = filtered_data['observation_type_concept_id'].map(concept_dict)
            # drop columns that are not needed 
            filtered_data.drop(columns=['person_source_value', 'visit_source_value'], inplace=True)            
            # only keep the columns that are not duplicates
            filtered_data = filtered_data.drop_duplicates(subset=['observation_id'], keep='first')
            # push the filtered data to the database
            asyncio.run(self.push_to_db(
                batch_size=250000,
                data=filtered_data,
                table_name=self._table
            ))            
            logging.info(f"Loaded data into table '{self._schema}.{self._table}'.")

        except Exception as e:
            logging.error(f"Failed to load data into table: {e}")