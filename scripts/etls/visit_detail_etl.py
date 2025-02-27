import pandas as pd
import logging
import uuid
from typing import Optional
from .main_etl import ETLEntity

class VisitDetail(ETLEntity):
    def map_data(self, mapper = {}):
        """Map the specific fields for the Visit detail table"""
        try:
            self._generate_ids()
            self._handle_dates()
            self._set_source_values()
            
            logging.info("Visit detail data mapped successfully.")
        except Exception as e:
            logging.error(f"Error during Visit detail data mapping: {e}")  

    def _set_source_values(self):
        """Set source values for OMOP mapping."""
        self._source_data['visit_detail_source_value'] = self._source_data['description']
        self._source_data['visit_detail_concept_id'] = self._source_data['code']
        self._source_data['provider_source_value'] = self._source_data['provider']
        
        self._source_data['care_site_source_value'] = self._source_data['organization'].apply(self.remove_non_alphanumeric)
        self._source_data['care_site_source_value'] = self._source_data['care_site_source_value'].apply(self.encrypt_value)
        
        self._source_data['visit_source_value'] = self._source_data['id']
        
        self._source_data['person_source_value'] = self._source_data['patient'].apply(self.remove_non_alphanumeric)
        self._source_data['person_source_value'] = self._source_data['person_source_value'].apply(self.encrypt_value)
        self._source_data['visit_detail_type_concept_id'] = 32817

        self._source_data['admitted_from_concept_id'] = self._source_data['reasoncode']
        self._source_data['admitted_from_source_value'] = self._source_data['reasondescription'].fillna('Unknown')
   
    def _handle_dates(self):
        """Ensure start and end dates are in datetime format."""
        self._source_data['visit_detail_start_datetime'] = pd.to_datetime(self._source_data['start'], errors='coerce')
        self._source_data['visit_detail_end_datetime'] = pd.to_datetime(self._source_data['stop'], errors='coerce')
        self._source_data['visit_detail_start_date'] = self._source_data['visit_detail_start_datetime'].dt.date
        self._source_data['visit_detail_end_date'] = self._source_data['visit_detail_end_datetime'].dt.date
        
    def _generate_ids(self):
        self._source_data['visit_detail_id'] = self._source_data['id'].apply(self.unique_id_generator, source_type='visit_detail')
