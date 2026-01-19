import pandas as pd
import logging
import uuid
from typing import Optional
from .main_etl import ETLEntity

class Location(ETLEntity):
    # pass the fields and their source
    
    def map_data(self, mapper = {}):
        """Map the specific fields for the Location entity."""
        try:
            self._generate_ids()
            self._set_source_values()
            logging.info("Location data mapped successfully.")
        except Exception as e:
            logging.error(f"Error during Location data mapping: {e}")

    def _generate_ids(self):
        # create another field here, call it lat_lon
        self._source_data['zip'] = self._source_data['zip'].fillna('').astype(str)
        self._source_data['zip'] = self._source_data['zip'].apply(self.remove_non_alphanumeric)
        self._source_data['location_id'] = self._source_data['zip'].apply(self.unique_id_generator, source_type='location')

    def _set_source_values(self):
        """Set source values for OMOP mapping."""
        # we are using the zip code here.
        self._source_data['location_source_value'] = self._source_data['zip'].apply(self.encrypt_value)
        self._source_data['city'] = self._source_data['city']
        self._source_data['county'] = self._source_data['state']
        self._source_data['country_source_value'] = "Ireland"
        self._source_data['country_concept_id'] = 4330438
