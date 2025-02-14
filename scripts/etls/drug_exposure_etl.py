import pandas as pd
import logging
import uuid
from typing import Optional
from .main_etl import ETLEntity

class DrugExposure(ETLEntity):
    def map_data(self, mapper = {}):
        """Map the specific fields for the Observation period table"""
        try:
            self._generate_ids()
            self._handle_dates()
            self._set_source_values()
            
            logging.info("Observation period data mapped successfully.")
        except Exception as e:
            logging.error(f"Error during observation period data mapping: {e}")  

    def _set_source_values(self):
        """Set source values for OMOP mapping."""
        self._source_data['drug_source_value'] = self._source_data['description']
        self._source_data['drug_source_concept_id'] = self._source_data['code']
        self._source_data['visit_source_value'] = self._source_data['encounter']
        self._source_data['person_source_value'] = self._source_data['patient'].apply(self.remove_non_alphanumeric)
        self._source_data['person_source_value'] = self._source_data['person_source_value'].apply(self.encrypt_value)
        self._source_data['drug_type_concept_id'] = 32817

    def _handle_dates(self):
        """Ensure start and end dates are in datetime format."""
        self._source_data['drug_exposure_start_datetime'] = pd.to_datetime(self._source_data['start'], errors='coerce')
        self._source_data['drug_exposure_end_datetime'] = pd.to_datetime(self._source_data['stop'], errors='coerce')
        self._source_data['drug_exposure_start_date'] = self._source_data['drug_exposure_start_datetime'].dt.date
        self._source_data['drug_exposure_end_date'] = self._source_data['drug_exposure_end_datetime'].dt.date

                
    def _generate_ids(self):
        self._source_data['drug_exposure_id'] = self._source_data['encounter'].apply(self.unique_id_generator, source_type='drug_exposure')
