import pandas as pd
import logging
import uuid
from typing import Optional
from .main_etl import ETLEntity

class Person(ETLEntity):
    # using python
    GENDER_MAP = {
        'm': 8507,
        'male': 8507,
        'f': 8532,
        'female': 8532
    }
    RACE_MAP = {
        'white': 8527,
        'black': 8516,
        'african american': 8516,
        'asian': 8515,
        'native': 8657,
        'american indian': 8657,
        'alaska native': 8657,
        'hawaiian': 8557,
        'other': 38003613
    }
    ETHNICITY_MAP = {
        'hispanic': 38003563,
        'hispanic or latino': 38003563,
        'nonhispanic': 38003564,
        'not hispanic or latino': 38003564
    }

    def map_data(self):
        """Map the specific fields for the Person entity."""
        try:
            self._generate_ids()
            self._normalize_demographics()
            self._map_gender()
            self._handle_birthdate()
            self._map_race_ethnicity()
            self._set_source_values()
            logging.info("person data mapped successfully.")
        except Exception as e:
            logging.error(f"Error during person data mapping: {e}")

    def _generate_ids(self):
        self._source_data['person_id'] = self._source_data['id'].apply(self.unique_id_generator, source_type='person')
        
    def _normalize_demographics(self):
        """Normalize gender, race, and ethnicity for mapping while preserving source values."""
        self._source_data['gender_source_value'] = self._source_data['gender']
        self._source_data['race_source_value'] = self._source_data['race']
        self._source_data['ethnicity_source_value'] = self._source_data['ethnicity']

        self._source_data['gender'] = (
            self._source_data['gender_source_value']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.lower()
        )
        self._source_data['race'] = (
            self._source_data['race_source_value']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.lower()
        )
        self._source_data['ethnicity'] = (
            self._source_data['ethnicity_source_value']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.lower()
        )

    def _map_gender(self):
        """Map gender to OMOP concepts."""
        self._source_data['gender_concept_id'] = (
            self._source_data['gender'].map(self.GENDER_MAP).fillna(0).astype(int)
        )

    def _handle_birthdate(self):
        """Ensure birthdate is in datetime format and extract year, month, day."""
        self._source_data['birthdate'] = pd.to_datetime(self._source_data['birthdate'], errors='coerce')
        self._source_data = self._source_data.dropna(subset=['birthdate'])
        self._source_data['year_of_birth'] = self._source_data['birthdate'].dt.year
        self._source_data['month_of_birth'] = self._source_data['birthdate'].dt.month
        self._source_data['day_of_birth'] = self._source_data['birthdate'].dt.day
        self._source_data['birth_datetime'] = self._source_data['birthdate']

    def _map_race_ethnicity(self):
        """Map race and ethnicity to OMOP concepts."""
        self._source_data['race_concept_id'] = (
            self._source_data['race'].map(self.RACE_MAP).fillna(0).astype(int)
        )
        self._source_data['ethnicity_concept_id'] = (
            self._source_data['ethnicity'].map(self.ETHNICITY_MAP).fillna(0).astype(int)
        )

    def _set_source_values(self):
        """Set source values for OMOP mapping."""
        self._source_data['gender_source_concept_id'] = 0
        self._source_data['race_source_concept_id'] = 0
        self._source_data['ethnicity_source_concept_id'] = 0
        self._source_data['person_source_value'] = self._source_data['id'].apply(self.remove_non_alphanumeric)
        self._source_data['person_source_value'] = self._source_data['person_source_value'].apply(self.encrypt_value)
        self._source_data['zip'] = self._source_data['zip'].fillna('').astype(str)
        self._source_data['zip'] = self._source_data['zip'].apply(self.remove_non_alphanumeric)
        self._source_data['location_source_value'] = self._source_data['zip'].apply(self.encrypt_value)
        self._source_data['location_id'] = self._source_data['zip'].apply(self.unique_id_generator, source_type='location')
        self._source_data['provider_id'] = pd.NA
        self._source_data['care_site_id'] = pd.NA
