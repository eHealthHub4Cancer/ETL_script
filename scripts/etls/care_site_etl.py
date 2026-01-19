import pandas as pd
import logging
import uuid
from typing import Optional
from .main_etl import ETLEntity

class CareSite(ETLEntity):
    
    def map_data(self, mapper = {}):
        """Map the specific fields for the Care site entity."""
        try:
            self._generate_ids()
            self._set_source_values()
            
            logging.info("Care site data mapped successfully.")
        except Exception as e:
            logging.error(f"Error during care site data mapping: {e}")
            
    def _generate_ids(self):
        self._source_data['zip'] = self._source_data['zip'].fillna('').astype(str)
        self._source_data['zip'] = self._source_data['zip'].apply(self.remove_non_alphanumeric)
        self._source_data['organization'] = self._source_data['organization'].apply(self.remove_non_alphanumeric)
        self._source_data['care_site_id'] = self._source_data['organization'].apply(self.unique_id_generator, source_type='care site')
            
    def _set_source_values(self):
        # set source values for OMOP mapping.
        self._source_data['care_site_source_value'] = self._source_data['organization'].apply(self.encrypt_value)
        self._source_data['care_site_name'] = self._source_data['name']
        self._source_data['place_of_service_concept_id'] = 38004446
        self._source_data['location_source_value'] = self._source_data['zip'].apply(self.encrypt_value)
