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

class LoadProcedure(LoadOmoppedData):
    def load_data(self):
        """Load procedure occurrence data."""
        try:
            query_utils = QueryUtils(self._conn, self._schema, self._table, self.get_csv_loader())
            # retrieve person records
            queried_person = query_utils.retrieve_persons()
            # join both tables using inner join.
            self._omopped_data = self._omopped_data.merge(queried_person, on='person_source_value', how='inner')
            # retrieve past procedures
            queried_procedures = query_utils.retrieve_procedures()
            # get unique procedures.
            existing_procedures = set(queried_procedures['procedure_occurrence_id'])
            # get existing visits
            filtered_data = self._omopped_data[
                ~self._omopped_data['procedure_occurrence_id'].isin(existing_procedures)
            ]
            # check if there are new records to insert
            if filtered_data.empty:
                logging.info("No new data to insert for procedure occurrence; all records already exist in the target table.")
                return
            
            queried_visits = query_utils.retrieve_visits()
            # merge
            filtered_data = filtered_data.merge(queried_visits, on='visit_source_value', how='left')
            # convert the procedure source concept id to string
            filtered_data['procedure_source_concept_id'] = filtered_data['procedure_source_concept_id'].astype(str)
            # get the concept id
            unique_code = filtered_data['procedure_source_concept_id'].unique().tolist()
            unique_concept_id = query_utils.retrieve_concept_id(code=unique_code, vocabulary=('SNOMED', 'CPT4', 'HCPCS'))
            # merge the concept id
            filtered_data['procedure_concept_id'] = filtered_data['procedure_source_concept_id'].map(unique_concept_id).astype(int)
            # get the source concept id
            unique_source_concept_id = query_utils.retrieve_source_concept_id(code=unique_code, vocabulary=('SNOMED','HemOnc','NAACCR'))
            # merge the source concept id
            filtered_data['procedure_source_concept_id'] = filtered_data['procedure_source_concept_id'].map(unique_source_concept_id).astype(int)
            # strip the length
            filtered_data['procedure_source_value'] = filtered_data['procedure_source_value'].apply(query_utils.strip_length)
            # drop columns that are not needed 
            filtered_data.drop(columns=['person_source_value', 'visit_source_value'], inplace=True)            
            # only keep the columns that are not duplicates
            filtered_data = filtered_data.drop_duplicates(subset=['procedure_occurrence_id'], keep='first')
            # push the filtered data to the database
            asyncio.run(self.push_to_db(
                batch_size=250000,
                data=filtered_data,
                table_name=self._table
            ))            
            logging.info(f"Loaded data into table '{self._schema}.{self._table}'.")

        except Exception as e:
            logging.error(f"Failed to load data into table: {e}")