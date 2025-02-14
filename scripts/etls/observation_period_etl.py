import pandas as pd
import logging
import uuid
from typing import Optional
from .main_etl import ETLEntity

class ObservationPeriod(ETLEntity):
    def map_data(self, mapper = {}):
        """Map the specific fields for the Observation period table"""
        try:
            self._generate_ids()
            self._handle_dates()
            # self._set_source_values()
            logging.info("Observation period data mapped successfully.")
        except Exception as e:
            logging.error(f"Error during observation period data mapping: {e}")  

    def _handle_dates(self):
        """Ensure start and end dates are in datetime format."""
        self._source_data['start'] = pd.to_datetime(self._source_data['start'], errors='coerce')
        self._source_data['stop'] = pd.to_datetime(self._source_data['stop'], errors='coerce')
        self._source_data['period_type_concept_id'] = 32827

        self._source_data = self._source_data.groupby('patient').agg(
            observation_period_start_date = ('start', 'min'),
            observation_period_end_date = ('stop', 'max'),
            period_type_concept_id = ('period_type_concept_id', 'first'),
            observation_period_id = ('observation_period_id', 'first'),
        ).reset_index()

        self._source_data['person_source_value'] = self._source_data['patient'].apply(self.remove_non_alphanumeric)
        self._source_data['person_source_value'] = self._source_data['person_source_value'].apply(self.encrypt_value)
        
    def _generate_ids(self):
        self._source_data['observation_period_id'] = self._source_data['patient'].apply(self.unique_id_generator, source_type='observation_period')

    