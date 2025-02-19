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

class LoadDrug(LoadOmoppedData):
    def load_data(self):
        """Load encounter data into the OMOP visit occurrence table."""
        try:
            query_utils = QueryUtils(self._conn, self._schema, self._table, self.get_csv_loader())
            # retrieve person records
            queried_person = query_utils.retrieve_persons()
            # join both tables using inner join.
            self._omopped_data = self._omopped_data.merge(queried_person, on='person_source_value', how='inner')
            # retrieve past drug records
            queried_drugs = query_utils.retrieve_drugs()
            # get unique drugs.
            existing_drugs = set(queried_drugs['drug_exposure_id'])
            # query existing drugs
            filtered_data = self._omopped_data[
                ~self._omopped_data['drug_exposure_id'].isin(existing_drugs)
            ]
            # check if there are new records to insert
            if filtered_data.empty:
                logging.info("No new data to insert for procedure occurrence; all records already exist in the target table.")
                return
            
            # retrieve visits
            queried_visits = query_utils.retrieve_visits()
            # merge
            filtered_data = filtered_data.merge(queried_visits, on='visit_source_value', how='inner')
            # existing visit details
            # # retrieve concepts
            queried_concepts = query_utils.retrieve_concepts()
            # get only snomed vocabularies
            queried_concepts = queried_concepts[queried_concepts['vocabulary_id'].isin(['RxNorm', 'CVX'])]
            # merge based on concept code.
            queried_concepts = queried_concepts.set_index('concept_code').to_dict()['concept_id']
            # map the concept ids
            filtered_data['drug_concept_id'] = filtered_data['drug_source_concept_id'].astype(str).map(queried_concepts)
            # drop columns that are not needed 
            filtered_data.drop(columns=['person_source_value', 'visit_source_value'], inplace=True)            
            # only keep the columns that are not duplicates
            filtered_data = filtered_data.drop_duplicates(subset=['drug_exposure_id'], keep='first')
            # push the filtered data to the database
            asyncio.run(self.push_to_db(
                batch_size=250000,
                data=filtered_data,
                table_name=self._table
            ))            
            logging.info(f"Loaded data into table '{self._schema}.{self._table}'.")

        except Exception as e:
            logging.error(f"Failed to load data into table: {e}")