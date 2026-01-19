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
drug_window_size = os.getenv("DRUG_WINDOW")
drug_window_size = int(drug_window_size)


# Configure logging
logging.basicConfig(level=logging.DEBUG)  # Use DEBUG level for detailed logging

class LoadDrug(LoadOmoppedData):

    def load_drug_era_data(self, window_size: int = 30):
        """Load drug era data into OMOP drug era table."""
        try:
            query_utils = QueryUtils(self._conn, self._schema, self._table, self.get_csv_loader(), self._vocab_schema)
            # retrieve drug exposure records
            queried_drug_exposure = query_utils.retrieve_drug_exposure()

            if queried_drug_exposure.empty:
                logging.info("No drug exposure records found in the database.")
                return

            sorted_data = queried_drug_exposure.sort_values(by=['person_id', 'drug_concept_id','drug_exposure_start_date'])
            
            # if drug start date is empty.
            sorted_data['drug_exposure_start_date'] = sorted_data['drug_exposure_start_date'].fillna(
                sorted_data.groupby('person_id')['drug_exposure_start_date'].transform('min')
            )
            # if start date is still empty, mask it with the earliest date
            mask = sorted_data['drug_exposure_start_date'].isna() & sorted_data['drug_exposure_end_date'].notna()
            sorted_data.loc[mask, 'drug_exposure_start_date'] = sorted_data.loc[mask, 'drug_exposure_end_date'] - pd.Timedelta(days=30)
            # if start date is still empty, fill it with the median date
            median_date = sorted_data['drug_exposure_start_date'].median()
            sorted_data['drug_exposure_start_date'] = sorted_data['drug_exposure_start_date'].fillna(median_date)
            
            # if drug end date is empty.
            sorted_data['drug_exposure_end_date'] = sorted_data['drug_exposure_end_date'].fillna(
                sorted_data.groupby('person_id')['drug_exposure_end_date'].transform('max')
            )
            # if end date is still empty, fill with corresponding start date + 30 days
            sorted_data['drug_exposure_end_date'] = sorted_data['drug_exposure_end_date'].fillna(
                sorted_data['drug_exposure_start_date'] + pd.Timedelta(days=30)
            )
            
            sorted_data['prev_date'] = sorted_data.groupby(['person_id', 'drug_concept_id'])['drug_exposure_end_date'].shift(1)
            sorted_data['new_era'] = (sorted_data['prev_date'].isna()
                                      ) | ((sorted_data['drug_exposure_start_date'] - sorted_data['prev_date']).dt.days > window_size)
            
            # get new era group
            sorted_data['era'] = sorted_data.groupby(['person_id', 'drug_concept_id'])['new_era'].cumsum()
            # group by era and get the min and max dates
            sorted_data = sorted_data.groupby(['person_id', 'drug_concept_id', 'era']).agg(
                drug_era_start_date=('drug_exposure_start_date', 'first'),
                drug_era_end_date=('drug_exposure_end_date', 'last'),
            ).reset_index()
            
            # compute the gap_days
            sorted_data['prev_era_days'] = sorted_data.groupby(['person_id', 'drug_concept_id'])['drug_era_start_date'].shift(1)
            sorted_data['gap_days'] = (sorted_data['drug_era_start_date'] - sorted_data['prev_era_days']).dt.days.fillna(0).astype(int)
            # combine person_id, drug_concept_id, start_date, and end_date to generate drug_era_id
            sorted_data['drug_era_source'] = sorted_data[['person_id', 'drug_concept_id', 'drug_era_start_date', 'drug_era_end_date']].astype(str).agg('_'.join, axis=1)
            # print(sorted_data['drug_era_source'])
            # generate drug_era_id
            sorted_data['drug_era_id'] = sorted_data['drug_era_source'].apply(query_utils.unique_id_generator, source_type='drug era')
            # get unique drug era ids
            # get past drug era records
            queried_drug_era = query_utils.retrieve_drug_era()
            existing_drug_era = set(queried_drug_era['drug_era_id'])
            # get unique drug era ids

            filtered_data = sorted_data[
                ~sorted_data['drug_era_id'].isin(existing_drug_era)
            ]
            # check if there are new records to insert
            if filtered_data.empty:
                logging.info("No new data to insert for drug era; all records already exist in the target table.")
                return
            # drop columns that are not needed
            filtered_data.drop(columns=['era', 'prev_era_days', 'drug_era_source'], inplace=True)
            # only keep the columns that are not duplicates
            filtered_data = filtered_data.drop_duplicates(subset=['drug_era_id'], keep='first')
            # push the filtered data to the database
            asyncio.run(self.push_to_db(
                batch_size=250000,
                data=filtered_data,
                table_name='drug_era'
            ))
            logging.info(f"Loaded data into table '{self._schema}.drug_era'.")

        except Exception as e:
            logging.error(f"Failed to load data into table: {e}")

    def load_data(self):
        """Load drug exposure data into OMOP drug exposure table."""
        try:
            query_utils = QueryUtils(self._conn, self._schema, self._table, self.get_csv_loader(), self._vocab_schema)
            # retrieve person records
            queried_person = query_utils.retrieve_persons()
            # join both tables using inner join.
            self._omopped_data = self._omopped_data.merge(queried_person, on='person_source_value', how='inner')
            # retrieve past drug records
            queried_drugs = query_utils.retrieve_drugs()
            # get unique drugs.
            existing_drugs = set(queried_drugs['drug_exposure_id'])
            # query existing drugs
            filtered_data = self._omopped_data[
                ~self._omopped_data['drug_exposure_id'].isin(existing_drugs)
            ]
            # check if there are new records to insert
            if filtered_data.empty:
                logging.info("No new data to insert for drug exposure; all records already exist in the target table.")
                # load data into the drug era table.
                self.load_drug_era_data(drug_window_size)
                return
            
            filtered_data = filtered_data.copy()
            filtered_data.loc[:, 'drug_exposure_start_date'] = filtered_data['drug_exposure_start_date'].fillna(filtered_data['drug_exposure_end_date'])
            filtered_data.loc[:, 'drug_exposure_start_date'].fillna(pd.to_datetime('2000-01-01'))
            filtered_data.loc[:, 'drug_exposure_end_date'] = filtered_data['drug_exposure_end_date'].fillna(filtered_data['drug_exposure_start_date'])
            # retrieve visits
            queried_visits = query_utils.retrieve_visits()
            # merge
            filtered_data = filtered_data.merge(queried_visits, on='visit_source_value', how='left')
            filtered_data['drug_source_concept_id'] = filtered_data['drug_source_concept_id'].astype(str)
            # retrieve concept id
            unique_concepts = filtered_data['drug_source_concept_id'].unique()
            unique_concepts = unique_concepts.tolist()
            concept_id_map = query_utils.retrieve_concept_id(unique_concepts, vocabulary=('RxNorm', 'CVX', 'RxNorm Extension'))
            
            filtered_data['drug_concept_id'] = filtered_data['drug_source_concept_id'].map(concept_id_map).astype(int)
            # retrieve source concepts
            source_concept_id_map = query_utils.retrieve_source_concept_id(unique_concepts, vocabulary=(
                'RxNorm', 'CVX', 'RxNorm Extension','HCPCS','CPT4', 'HemOnc', 'NAACCR'))
            filtered_data['drug_source_concept_id'] = filtered_data['drug_source_concept_id'].map(source_concept_id_map).astype(int)
            # strip the length
            filtered_data['drug_source_value'] = filtered_data['drug_source_value'].apply(query_utils.strip_length)
            # drop columns that are not needed
            filtered_data.drop(columns=['person_source_value', 'visit_source_value'], inplace=True)            
            # only keep the columns that are not duplicates
            filtered_data = filtered_data.drop_duplicates(subset=['drug_exposure_id'], keep='first')
            # push the filtered data to the database
            asyncio.run(self.push_to_db(
                batch_size=250000,
                data=filtered_data,
                table_name=self._table
            ))
            # load data into the drug era table.            
            logging.info(f"Loaded data into table '{self._schema}.{self._table}'.")
            self.load_drug_era_data(drug_window_size)

        except Exception as e:
            logging.error(f"Failed to load data into table: {e}")