import pandas as pd
import logging
import uuid
from typing import Optional
from .main_etl import ETLEntity

class Observation(ETLEntity):
    CATEGORY_MAP = {
            'laboratory': 'LP29693-6',
            'vital-signs': 'LP30605-7',
            'exam': 'LP7801-6',
            'other': '23658-8'
        }
    
    QUALITY_MAP = {
        'QOLS': '1156447008',
        'QALY': '1156447008',
        'DALY': '1013870'
    }
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
        self._source_data['observation_source_value'] = self._source_data['description']
        self._source_data['visit_source_value'] = self._source_data['encounter']
        self._source_data['person_source_value'] = self._source_data['patient'].apply(self.remove_non_alphanumeric)
        self._source_data['person_source_value'] = self._source_data['person_source_value'].apply(self.encrypt_value)
        self._source_data['observation_concept_id'] = self._source_data['code'].map(lambda x: self.QUALITY_MAP.get(x, x))
        self._source_data['observation_type_concept_id'] = self._source_data['category'].map(lambda x: self.CATEGORY_MAP.get(x, x))

    def _handle_dates(self):
        """Ensure start and end dates are in datetime format."""
        self._source_data['observation_datetime'] = pd.to_datetime(self._source_data['date'], errors='coerce')
        self._source_data['observation_date'] = self._source_data['observation_datetime'].dt.date
                
    def _generate_ids(self):
        self._source_data['value'] = self._source_data['value'].replace('Acute myeloid leukemia  disease (disorder)', '0.0')
        self._source_data['category'] = self._source_data['category'].fillna('other')
        self._source_data['encounter'] = self._source_data['encounter'].fillna('3637e207-a102-5065-71b0-7420e18b1b5f')
        self._source_data['observation_id'] = self._source_data['encounter'].apply(self.unique_id_generator, source_type='observation')
