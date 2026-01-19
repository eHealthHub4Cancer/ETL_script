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

class LoadMeasurement(LoadOmoppedData):
    def load_data(self):
        """Load measurement data into the OMOP Measurement table."""
        try:
            query_utils = QueryUtils(self._conn, self._schema, self._table, self.get_csv_loader(), self._vocab_schema)
            # retrieve person records
            queried_person = query_utils.retrieve_persons()
            # join both tables using inner join.
            self._omopped_data = self._omopped_data.merge(queried_person, on='person_source_value', how='inner')
            # retrieve past measurement records
            queried_measurements = query_utils.retrieve_measurements()
            # get unique measurements.
            existing_measurements = set(queried_measurements['measurement_id'])
            # query existing measurements
            filtered_data = self._omopped_data[
                ~self._omopped_data['measurement_id'].isin(existing_measurements)
            ]
            # check if there are new records to insert
            if filtered_data.empty:
                logging.info("No new data to insert for measurement; all records already exist in the target table.")
                return     
            # retrieve visits
            queried_visits = query_utils.retrieve_visits()
            # merge on visit source value
            filtered_data = filtered_data.merge(queried_visits, on='visit_source_value', how='left')
            # get measurement unique concepts
            filtered_data['measurement_concept_id'] = filtered_data['measurement_concept_id'].astype(str)
            # get the unique concept id
            unique_code = filtered_data['measurement_concept_id'].unique().tolist()
            unique_concept_id = query_utils.retrieve_concept_id(code=unique_code, vocabulary=('SNOMED','LOINC'))
            # merge the concept id
            filtered_data['measurement_concept_id'] = filtered_data['measurement_concept_id'].map(unique_concept_id).astype(int)
            # convert the type concept id to string
            filtered_data['measurement_type_concept_id'] = filtered_data['measurement_type_concept_id'].astype(str)
            # get the unique concept id
            unique_types = filtered_data['measurement_type_concept_id'].unique().tolist()
            unique_type_id = query_utils.retrieve_concept_id(code=unique_types, vocabulary=('SNOMED', 'LOINC'))
            # merge the concept id
            filtered_data['measurement_type_concept_id'] = filtered_data['measurement_type_concept_id'].map(unique_type_id).astype(int)
            # strip the length
            filtered_data['measurement_source_value'] = filtered_data['measurement_source_value'].apply(query_utils.strip_length)
            # drop columns that are not needed 
            filtered_data.drop(columns=['person_source_value', 'visit_source_value'], inplace=True)            
            # only keep the columns that are not duplicates
            filtered_data = filtered_data.drop_duplicates(subset=['measurement_id'], keep='first')
            # push the filtered data to the database
            asyncio.run(self.push_to_db(
                batch_size=250000,
                data=filtered_data,
                table_name=self._table
            ))            
            logging.info(f"Loaded data into table '{self._schema}.{self._table}'.")

        except Exception as e:
            logging.error(f"Failed to load data into table: {e}")