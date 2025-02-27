import pandas as pd
# loading ...
import logging
import uuid
from typing import Optional
from .main_etl import ETLEntity

class Provider(ETLEntity):
    # using python
    GENDER_MAP = {'M': 8507, 'F': 8532} 

    def map_data(self):
        """Map the specific fields for the Person entity."""
        try:
            self._generate_ids()
            self._map_gender()
            self._map_concept_ids()
            self._set_source_values()

            # print(self._source_data.columns)

            logging.info("person data mapped successfully.")
        except Exception as e:
            logging.error(f"Error during person data mapping: {e}")

    def _generate_ids(self):
        self._source_data['provider_id'] = self._source_data['id'].apply(self.unique_id_generator, source_type='provider')
        
    def _map_gender(self):
        """Map gender to OMOP concepts."""
        self._source_data['gender_concept_id'] = self._source_data['gender'].map(self.GENDER_MAP)

    def _map_concept_ids(self):
        """ map the concept ids for the provider entity"""
        self._source_data['specialty_concept_id'] = 4216158

    def _set_source_values(self):
        """Set source values for OMOP mapping."""
        self._source_data['provider_name'] = self._source_data['name']
        self._source_data['gender_source_value'] = self._source_data['gender']
        self._source_data['provider_source_value'] = self._source_data['id']
        self._source_data['specialty_source_value'] = self._source_data['speciality']
        self._source_data['care_site_source_value'] = self._source_data['organization'].apply(self.remove_non_alphanumeric)
        self._source_data['care_site_source_value'] = self._source_data['care_site_source_value'].apply(self.encrypt_value)
