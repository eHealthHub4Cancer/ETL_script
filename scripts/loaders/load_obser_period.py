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

class LoadObservationPeriod(LoadOmoppedData):
    def load_data(self):
        """Load Observation data into the OMOP ObservationPeriod table."""
        try:
            # retrieve existing person_source_value records
            query_utils = QueryUtils(self._conn, self._schema, self._table, self.get_csv_loader(), self._vocab_schema)
            # generate person values
            retrieved_persons = query_utils.retrieve_person_birthdates()
            # merge the data
            self._omopped_data = self._omopped_data.merge(retrieved_persons, on='person_source_value', how='inner')
            if 'birth_datetime' in self._omopped_data.columns:
                birth_date = pd.to_datetime(self._omopped_data['birth_datetime'], errors='coerce').dt.date
                self._omopped_data['observation_period_start_date'] = self._omopped_data[
                    'observation_period_start_date'
                ].where(
                    birth_date.isna() | (self._omopped_data['observation_period_start_date'] >= birth_date),
                    birth_date,
                )
                self._omopped_data['observation_period_end_date'] = self._omopped_data[
                    'observation_period_end_date'
                ].where(
                    self._omopped_data['observation_period_end_date'] >= self._omopped_data['observation_period_start_date'],
                    self._omopped_data['observation_period_start_date'],
                )
            self._omopped_data.drop(columns=['person_source_value', 'birth_datetime'], inplace=True, errors='ignore')
            # Retrieve existing observation period records
            queried_data_pandas = query_utils.retrieve_obser_periods()
            # Initialize an empty set and update with existing values
            existing_values = set(queried_data_pandas['person_id'])
            # Filter the new data to only include unique person_id entries
            filtered_data = self._omopped_data[
                ~self._omopped_data['person_id'].isin(existing_values)
            ]
            if filtered_data.empty:
                logging.info("No new data to insert for observation period; all records already exist in the target table.")
                return
            # only keep the columns that are not duplicates
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
