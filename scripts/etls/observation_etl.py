import pandas as pd
import logging
import uuid
from typing import Optional
from .main_etl import ETLEntity

class Observation(ETLEntity):
    CATEGORY_MAP = {
            'health indicator': '723111000000104'
        }
    
    QUALITY_MAP = {
        'QOLS': 'LP156440-2',
        'QALY': '273724008',
        'DALY': 'D000087509'
    }

    def map_data(self, mapper = {}):
        """Map the specific fields for the Observation table"""
        
        try:
            # print all nulls
            self._generate_ids()
            self._set_category()
            self._handle_dates()
            self._set_source_values()
            
            logging.info("Observation data mapped successfully.")
        except Exception as e:
            logging.error(f"Error during observation data mapping: {e}")  

    def _set_category(self):
        self._source_data['observation_type_concept_id'] = self._source_data['category'].map(self.CATEGORY_MAP)
        # remove null rows
        self._source_data = self._source_data.dropna(subset=['observation_type_concept_id'])
    
    def _set_source_values(self):
        """Set source values for OMOP mapping."""
        self._source_data['observation_source_value'] = self._source_data['description']
        self._source_data['visit_source_value'] = self._source_data['encounter']
        self._source_data['person_source_value'] = self._source_data['patient'].apply(self.remove_non_alphanumeric)
        self._source_data['person_source_value'] = self._source_data['person_source_value'].apply(self.encrypt_value)
        self._source_data['observation_concept_id'] = self._source_data['code'].map(lambda x: self.QUALITY_MAP.get(x, x))
        self._source_data['value_as_number'] = self._source_data['value']
        self._source_data['value_source_value'] = self._source_data['value'].astype(str)
        self._source_data['unit_source_value'] = self._source_data['units']

    def _handle_dates(self):
        """Ensure start and end dates are in datetime format."""
        self._source_data['observation_datetime'] = pd.to_datetime(self._source_data['date'], errors='coerce')
        self._source_data['observation_date'] = self._source_data['observation_datetime'].dt.date
                
    def _generate_ids(self):
        self._source_data['value'] = pd.to_numeric(self._source_data['value'], errors='coerce').fillna(0.0)
        self._source_data['category'] = self._source_data['category'].fillna('health indicator')
        self._source_data['encounter'] = self._source_data['encounter'].fillna('3637e207-a102-5065-71b0-7420e18b1b5f')
        self._source_data['observation_id'] = self._source_data['encounter'].apply(self.unique_id_generator, source_type='observation')
