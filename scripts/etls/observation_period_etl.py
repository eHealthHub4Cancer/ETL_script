import pandas as pd
import logging
import uuid
from typing import Optional
from .main_etl import ETLEntity

class ObservationPeriod(ETLEntity):
    def map_data(self):
        """Map the specific fields for the Observation period table"""
        try:
            self._generate_person_ids()
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
            observation_period_id = ('observation_period_id', 'first')

        ).reset_index()
        
        # Fill missing dates with default values
        self._source_data['observation_period_start_date'] = self._source_data['observation_period_start_date'].fillna(pd.Timestamp('1900-01-01'))
        self._source_data['observation_period_end_date'] = self._source_data['observation_period_end_date'].fillna(pd.Timestamp('2070-01-01'))

        # Validation: Check if end date is earlier than start date
        invalid_periods = self._source_data[
            self._source_data['observation_period_end_date'] < self._source_data['observation_period_start_date']
        ]

        if not invalid_periods.empty:
            logging.warning(f"Found {len(invalid_periods)} invalid observation periods.")
            logging.warning("End date must be greater than or equal to start data")
            logging.warning(invalid_periods)
            raise
            
    def _generate_person_ids(self):
        self._source_data['observation_period_id'] = self._source_data['patient'].apply(self.unique_id_generator, source_type='observation_period')

    