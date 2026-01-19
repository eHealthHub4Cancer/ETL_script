from .main_load import LoadOmoppedData
import logging
from typing import Optional
import pandas as pd
import numpy as np
import rpy2.robjects as ro
from rpy2.robjects.packages import importr
from rpy2.robjects import pandas2ri
from .query_utils import QueryUtils
import asyncio
from dotenv import load_dotenv
import os

load_dotenv()

def _get_int_env(name: str, default: int) -> int:
    value = os.getenv(name, str(default))
    try:
        return int(value)
    except (TypeError, ValueError):
        return default

condition_window_size = _get_int_env("CONDITION_WINDOW", 30)

# Configure logging
logging.basicConfig(level=logging.DEBUG)  # Use DEBUG level for detailed logging

class LoadCondition(LoadOmoppedData):

    def load_condition_era(self, window_size: int = 30):
        """Load drug era data into OMOP drug era table."""
        try:
            
            query_utils = QueryUtils(self._conn, self._schema, self._table, self.get_csv_loader(), self._vocab_schema)
            # retrieve drug exposure records
            queried_condition_era = query_utils.retrieve_condition_occurrence()
            if queried_condition_era.empty:
                logging.info("No Condition Occurrence records found in the database.")
                return

            sorted_data = queried_condition_era.sort_values(by=['person_id', 'condition_concept_id','condition_start_date'])
            # if condition end date is not in the column, create one
            if 'condition_end_date' not in sorted_data.columns:
                sorted_data['condition_end_date'] = sorted_data['condition_start_date'] + pd.Timedelta(days=30)
            
            # step 1: get earliest start date
            min_start_date = sorted_data.groupby('person_id')['condition_start_date'].transform('min')
            # sort each person data by the earlies
            sorted_data['condition_start_date'] = sorted_data['condition_start_date'].fillna(
                sorted_data['person_id'].map(min_start_date)
            )
            # step 2: if the start date is still empty, fill it with the earliest date
            mask = sorted_data['condition_start_date'].isna() & sorted_data['condition_end_date'].notna()
            sorted_data.loc[mask, 'condition_start_date'] = sorted_data.loc[mask, 'condition_end_date'] - pd.Timedelta(days=30)

            # step 3: if the end date is empty, fill it with the median date
            # get the median date
            median_date = sorted_data['condition_start_date'].median()
            sorted_data['condition_start_date'] = sorted_data['condition_start_date'].fillna(median_date)

            # if condition end date is still empty, fill with corresponding start date + 30 days
            sorted_data['condition_end_date'] = sorted_data['condition_end_date'].fillna(
                sorted_data['condition_start_date'] + pd.Timedelta(days=30)
            )

            sorted_data['prev_date'] = sorted_data.groupby(['person_id', 'condition_concept_id'])['condition_end_date'].shift(1)
            sorted_data['new_era'] = (sorted_data['prev_date'].isna()
                                      ) | ((sorted_data['condition_start_date'] - sorted_data['prev_date']).dt.days > window_size)
            # get new era group
            sorted_data['era'] = sorted_data.groupby(['person_id', 'condition_concept_id'])['new_era'].cumsum()
            # group by era and get the min and max dates
            sorted_data = sorted_data.groupby(['person_id', 'condition_concept_id', 'era']).agg(
                condition_era_start_date=('condition_start_date', 'first'),
                condition_era_end_date=('condition_end_date', 'last'),
            ).reset_index()

            # compute the gap_days
            sorted_data['prev_era_days'] = sorted_data.groupby(['person_id', 'condition_concept_id'])['condition_era_start_date'].shift(1)
            sorted_data['gap_days'] = (sorted_data['condition_era_start_date'] - sorted_data['prev_era_days']).dt.days.fillna(0).astype(int)
            # combine person_id, condition_concept_id, start_date, and end_date to generate condition_era_id
            sorted_data['condition_era_source'] = sorted_data[['person_id', 'condition_concept_id', 'condition_era_start_date', 'condition_era_end_date']].astype(str).agg('_'.join, axis=1)
            # generate condition_era_id
            sorted_data['condition_era_id'] = sorted_data['condition_era_source'].apply(query_utils.unique_id_generator, source_type='condition era')
            # get past condition era records
            queried_condition_era = query_utils.retrieve_condition_era()
            existing_condition_era = set(queried_condition_era['condition_era_id'])
            # get unique drug era ids
            filtered_data = sorted_data[
                ~sorted_data['condition_era_id'].isin(existing_condition_era)
            ]
            # check if there are new records to insert
            if filtered_data.empty:
                logging.info("No new data to insert for drug era; all records already exist in the target table.")
                return
            # drop columns that are not needed
            filtered_data.drop(columns=['era', 'prev_era_days', 'condition_era_source','gap_days'], inplace=True)
            # avoid duplicates
            filtered_data = filtered_data.drop_duplicates(subset=['condition_era_id'], keep='first')
            # push the filtered data to the database
            asyncio.run(self.push_to_db(
                batch_size=250000,
                data=filtered_data,
                table_name='condition_era'
            ))
            logging.info(f"Loaded data into table '{self._schema}.condition_era'.")

        except Exception as e:
            logging.error(f"Failed to load data into table: {e}")


    def load_data(self):
        """Load condition into condition occurrence table."""
        try:
            query_utils = QueryUtils(self._conn, self._schema, self._table, self.get_csv_loader(), self._vocab_schema)
            # retrieve person records
            queried_person = query_utils.retrieve_persons()
            # join both tables using inner join.
            self._omopped_data = self._omopped_data.merge(queried_person, on='person_source_value', how='inner')
            # retrieve past conditions
            queried_conditions = query_utils.retrieve_conditions()
            # get unique conditions.
            existing_conditions = set(queried_conditions['condition_occurrence_id'])
            # get existing visits
            filtered_data = self._omopped_data[
                ~self._omopped_data['condition_occurrence_id'].isin(existing_conditions)
            ]
            # check if there are new records to insert
            if filtered_data.empty:
                logging.info("No new data to insert for condition occurrence; all records already exist in the target table.")
                self.load_condition_era(condition_window_size)
                return
            
            queried_visits = query_utils.retrieve_visits()
            # merge
            filtered_data = filtered_data.merge(queried_visits, on='visit_source_value', how='left')
            # convert the condition source concept id to string
            filtered_data['condition_source_concept_id'] = filtered_data['condition_source_concept_id'].astype(str)
            # get the concept id
            unique_code = filtered_data['condition_source_concept_id'].unique().tolist()
            unique_concept_id = query_utils.retrieve_concept_id(code=unique_code, vocabulary=('SNOMED', 'ICD10CM', 'ICD9CM'))
            # merge the concept id
            filtered_data['condition_concept_id'] = filtered_data['condition_source_concept_id'].map(unique_concept_id).astype(int)
            # get the source concept id
            unique_source_concept_id = query_utils.retrieve_source_concept_id(code=unique_code, vocabulary=('SNOMED', 'ICD10CM', 'ICD9CM'))
            # merge the source concept id
            filtered_data['condition_source_concept_id'] = filtered_data['condition_source_concept_id'].map(unique_source_concept_id).astype(int)
            # strip the length
            filtered_data['condition_source_value'] = filtered_data['condition_source_value'].apply(query_utils.strip_length)
            filtered_data.drop(columns=['person_source_value', 'visit_source_value'], inplace=True)            
            # only keep the columns that are not duplicates
            filtered_data = filtered_data.drop_duplicates(subset=['condition_occurrence_id'], keep='first')
            # push the filtered data to the database
            asyncio.run(self.push_to_db(
                batch_size=250000,
                data=filtered_data,
                table_name=self._table
            ))            
            logging.info(f"Loaded data into table '{self._schema}.{self._table}'.")
            self.load_condition_era(condition_window_size)
        except Exception as e:
            logging.error(f"Failed to load data into table: {e}")
