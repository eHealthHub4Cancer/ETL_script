import pandas as pd
import logging
import uuid
from typing import Optional
from .main_etl import ETLEntity

class Death(ETLEntity):
    # pass the fields and their source
    
    def map_data(self, mapper = {}):
        """Map the specific fields for the Death entity."""
        try:
            self._remove_non_death_records()
            self._handle_dates()
            self._set_source_values()
            logging.info("Death data mapped successfully.")
        except Exception as e:
            logging.error(f"Error during Death data mapping: {e}")

    def _remove_non_death_records(self):
        """Remove records that have empty death dates."""
        self._source_data = self._source_data.dropna(subset=['deathdate'])
            
    def _handle_dates(self):
        """Ensure start and end dates are in datetime format."""
        self._source_data['death_datetime'] = pd.to_datetime(self._source_data['deathdate'], errors='coerce')
        self._source_data = self._source_data.dropna(subset=['death_datetime'])
        # convert to date
        self._source_data['death_date'] = self._source_data['death_datetime'].dt.date
        
    def _set_source_values(self):
        """Set source values for OMOP mapping."""
        self._source_data['death_type_concept_id'] = 32817 # EHR record
        self._set_cause_of_death()
        self._source_data['person_source_value'] = self._source_data['id'].apply(self.remove_non_alphanumeric)
        self._source_data['person_source_value'] = self._source_data['person_source_value'].apply(self.encrypt_value)

    def _set_cause_of_death(self):
        """Set cause of death fields without hardcoding a specific condition."""
        cause_columns = [
            'cause_of_death',
            'death_cause',
            'deathcause',
            'cause',
            'cause_source_value'
        ]

        cause_column = next((col for col in cause_columns if col in self._source_data.columns), None)

        if cause_column:
            self._source_data['cause_source_value'] = self._source_data[cause_column]
        else:
            self._source_data['cause_source_value'] = None

        if 'cause_concept_id' not in self._source_data.columns:
            self._source_data['cause_concept_id'] = 0
        if 'cause_source_concept_id' not in self._source_data.columns:
            self._source_data['cause_source_concept_id'] = 0
