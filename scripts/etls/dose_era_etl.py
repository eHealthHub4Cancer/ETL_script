import asyncio
import logging
import pandas as pd


class DoseEraETL:
    def __init__(self, query_utils, push_to_db, schema: str):
        self._query_utils = query_utils
        self._push_to_db = push_to_db
        self._schema = schema

    def build(self, window_size: int = 30):
        """Load dose era data into OMOP dose_era table."""
        try:
            queried_drug_exposure = self._query_utils.retrieve_drug_exposure()
            if queried_drug_exposure.empty:
                logging.info("No drug exposure records found in the database.")
                return

            sorted_data = queried_drug_exposure.sort_values(
                by=['person_id', 'drug_concept_id', 'drug_exposure_start_date']
            )
            sorted_data['drug_exposure_start_date'] = sorted_data['drug_exposure_start_date'].fillna(
                sorted_data.groupby('person_id')['drug_exposure_start_date'].transform('min')
            )
            mask = sorted_data['drug_exposure_start_date'].isna() & sorted_data['drug_exposure_end_date'].notna()
            sorted_data.loc[mask, 'drug_exposure_start_date'] = (
                sorted_data.loc[mask, 'drug_exposure_end_date'] - pd.Timedelta(days=30)
            )
            median_date = sorted_data['drug_exposure_start_date'].median()
            sorted_data['drug_exposure_start_date'] = sorted_data['drug_exposure_start_date'].fillna(median_date)
            sorted_data['drug_exposure_end_date'] = sorted_data['drug_exposure_end_date'].fillna(
                sorted_data.groupby('person_id')['drug_exposure_end_date'].transform('max')
            )
            sorted_data['drug_exposure_end_date'] = sorted_data['drug_exposure_end_date'].fillna(
                sorted_data['drug_exposure_start_date'] + pd.Timedelta(days=30)
            )

            if 'dose_unit_concept_id' in sorted_data.columns:
                sorted_data['unit_concept_id'] = sorted_data['dose_unit_concept_id'].fillna(0).astype(int)
            else:
                sorted_data['unit_concept_id'] = 0

            if 'dose_value' in sorted_data.columns:
                sorted_data['dose_value'] = pd.to_numeric(sorted_data['dose_value'], errors='coerce')
            elif 'quantity' in sorted_data.columns:
                sorted_data['dose_value'] = pd.to_numeric(sorted_data['quantity'], errors='coerce')
            else:
                sorted_data['dose_value'] = None

            sorted_data['dose_value'] = sorted_data['dose_value'].fillna(1.0)

            sorted_data['prev_date'] = sorted_data.groupby(
                ['person_id', 'drug_concept_id', 'unit_concept_id', 'dose_value']
            )['drug_exposure_end_date'].shift(1)
            sorted_data['new_era'] = (sorted_data['prev_date'].isna()) | (
                (sorted_data['drug_exposure_start_date'] - sorted_data['prev_date']).dt.days > window_size
            )
            sorted_data['era'] = sorted_data.groupby(
                ['person_id', 'drug_concept_id', 'unit_concept_id', 'dose_value']
            )['new_era'].cumsum()
            sorted_data = sorted_data.groupby(
                ['person_id', 'drug_concept_id', 'unit_concept_id', 'dose_value', 'era']
            ).agg(
                dose_era_start_date=('drug_exposure_start_date', 'first'),
                dose_era_end_date=('drug_exposure_end_date', 'last'),
            ).reset_index()

            sorted_data['dose_era_source'] = sorted_data[
                ['person_id', 'drug_concept_id', 'unit_concept_id', 'dose_value', 'dose_era_start_date', 'dose_era_end_date']
            ].astype(str).agg('_'.join, axis=1)
            sorted_data['dose_era_id'] = sorted_data['dose_era_source'].apply(
                self._query_utils.unique_id_generator, source_type='dose era'
            )

            queried_dose_era = self._query_utils.retrieve_dose_era()
            existing_dose_era = set(queried_dose_era['dose_era_id'])
            filtered_data = sorted_data[~sorted_data['dose_era_id'].isin(existing_dose_era)]
            if filtered_data.empty:
                logging.info("No new data to insert for dose era; all records already exist in the target table.")
                return

            filtered_data.drop(columns=['era', 'dose_era_source'], inplace=True)
            filtered_data = filtered_data.drop_duplicates(subset=['dose_era_id'], keep='first')

            asyncio.run(self._push_to_db(
                batch_size=250000,
                data=filtered_data,
                table_name='dose_era'
            ))
            logging.info(f"Loaded data into table '{self._schema}.dose_era'.")

        except Exception as e:
            logging.error(f"Failed to load data into table: {e}")
