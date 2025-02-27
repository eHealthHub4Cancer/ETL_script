import pandas as pd
# loading...
import logging
import uuid
from typing import Optional
from .main_etl import ETLEntity

class Encounters(ETLEntity):
    ENCOUNTER_CLASS_MAP = {'inpatient': 9201, 'outpatient': 9202, 'wellness': 9202,
                           'ambulatory': 38004207, 
                           'emergency': 9203, 'urgentcare': 8782}

    def map_data(self, mapper = {}):
        """Map the specific fields for the visit occurrence entity."""
        try:
            self._generate_ids()
            self._map_visit_concept()
            self._map_visit_type()
            self._set_source_values()
            self._handle_visit_dates()
            self._aggregate_data()

            logging.info("Visit Occurrence data mapped successfully.")
        except Exception as e:
            logging.error(f"Error during Visit Occurrence data mapping: {e}")
            
    def _generate_ids(self):
        self._source_data['visit_occurrence_id'] = self._source_data['id'].apply(self.unique_id_generator, source_type='visit occurrence')
        
    def _map_visit_concept(self):
        """Map gender to OMOP concepts."""
        self._source_data['visit_concept_id'] = self._source_data['encounterclass'].map(self.ENCOUNTER_CLASS_MAP)
        self._source_data['visit_concept_id'] = self._source_data['visit_concept_id'].fillna('9201')
        self._source_data['visit_concept_id'] = self._source_data['visit_concept_id'].astype(int)

    def _handle_visit_dates(self):
        """Ensure birthdate is in datetime format and extract year, month, day."""
        self._source_data['start'] = pd.to_datetime(self._source_data['start'], errors='coerce')
        self._source_data['stop'] = pd.to_datetime(self._source_data['stop'], errors='coerce')
        # start handling dates.
        self._source_data['visit_start_date'] = self._source_data['start'].dt.date
        self._source_data['visit_start_datetime'] = self._source_data['start']
        # for end dates.
        self._source_data['visit_end_date'] = self._source_data['stop'].dt.date
        self._source_data['visit_end_datetime'] = self._source_data['stop']

    def _map_visit_type(self):
        # set the visit type concept id.
        self._source_data['visit_type_concept_id'] = 32827
    
    def _set_source_values(self):
        # set source values for OMOP mapping.
        self._source_data['person_source_value'] = self._source_data['patient'].apply(self.remove_non_alphanumeric)
        self._source_data['person_source_value'] = self._source_data['person_source_value'].apply(self.encrypt_value)
        # do for organization.
        self._source_data['provider_source_value'] = self._source_data['provider']
        self._source_data['care_site_source_value'] = self._source_data['organization'].apply(self.remove_non_alphanumeric)
        self._source_data['care_site_source_value'] = self._source_data['care_site_source_value'].apply(self.encrypt_value)
        self._source_data['visit_source_value'] = self._source_data['id']

    def _aggregate_data(self, gap_threshold = 1):
        """Aggregate data by person_source_value and visit_concept_id."""

        # sort data before processing gaps.
        self._source_data = self._source_data.sort_values(['person_source_value', 'visit_concept_id', 'visit_start_date'])
        # calculate the gap between visits.
        self._source_data['prev_visit_end_date'] = self._source_data.groupby(['person_source_value', 'visit_concept_id'])['visit_end_datetime'].shift(1)
        self._source_data['visit_gap'] = (self._source_data['visit_start_datetime'] - self._source_data['prev_visit_end_date']).dt.days.fillna(0)
        # filter out visits that are within the gap threshold.
        self._source_data['new_visit'] = (self._source_data['visit_gap'] > gap_threshold).astype(int)
        self._source_data['new_visit'] = self._source_data['new_visit'].cumsum()

        print(self._source_data)

        self._source_data = self._source_data.groupby(['person_source_value', 'visit_concept_id', 'new_visit']).agg(
            visit_start_date = ('visit_start_date', 'min'),
            visit_end_date = ('visit_end_date', 'max'),
            visit_start_datetime = ('visit_start_datetime', 'min'),
            visit_end_datetime = ('visit_end_datetime', 'max'),
            visit_type_concept_id = ('visit_type_concept_id', 'first'),
            visit_occurrence_id = ('visit_occurrence_id', 'first'),
            care_site_source_value = ('care_site_source_value', 'first'),
            provider_source_value = ('provider_source_value', 'first'),
            visit_source_value = ('visit_source_value', 'first')
        ).reset_index(drop=True)