import pandas as pd
import logging
import uuid
from typing import Optional
from .main_etl import ETLEntity

class Death(ETLEntity):
    # pass the fields and their source
    
    def map_data(self, mapper = {}):
        """Map the specific fields for the Person entity."""
        try:
            self._remove_non_death_records()
            self._handle_dates()
            self._set_source_values()
            logging.info("person data mapped successfully.")
        except Exception as e:
            logging.error(f"Error during person data mapping: {e}")

    def _remove_non_death_records(self):
        """Remove records that have empty death dates."""
        self._source_data = self._source_data.dropna(subset=['deathdate'])
            
    def _handle_dates(self):
        """Ensure start and end dates are in datetime format."""
        self._source_data['death_datetime'] = pd.to_datetime(self._source_data['deathdate'], errors='coerce')
        # convert to date
        self._source_data['death_date'] = self._source_data['death_datetime'].dt.date
        
    def _set_source_values(self):
        """Set source values for OMOP mapping."""
        self._source_data['death_type_concept_id'] = 32817 # EHR record
        self._source_data['cause_source_value'] = 'C92.0' # ICD-10 code for Acute myeloblastic leukemia
        self._source_data['cause_concept_id'] = 140352 # Acute myeloblastic leukemia
        self._source_data['cause_source_concept_id'] = 45600565 # ICD-10
        self._source_data['person_source_value'] = self._source_data['id'].apply(self.remove_non_alphanumeric)
        self._source_data['person_source_value'] = self._source_data['person_source_value'].apply(self.encrypt_value)