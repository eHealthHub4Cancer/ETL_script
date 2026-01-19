import asyncio
import logging
import pandas as pd


class ConditionEraETL:
    def __init__(self, query_utils, push_to_db, schema: str):
        self._query_utils = query_utils
        self._push_to_db = push_to_db
        self._schema = schema

    def build(self, window_size: int = 30):
        """Load condition era data into OMOP condition_era table."""
        try:
            queried_condition_occurrence = self._query_utils.retrieve_condition_occurrence()
            if queried_condition_occurrence.empty:
                logging.info("No Condition Occurrence records found in the database.")
                return

            sorted_data = queried_condition_occurrence.sort_values(
                by=['person_id', 'condition_concept_id', 'condition_start_date']
            )

            if 'condition_end_date' not in sorted_data.columns:
                sorted_data['condition_end_date'] = sorted_data['condition_start_date'] + pd.Timedelta(days=30)

            min_start_date = sorted_data.groupby('person_id')['condition_start_date'].transform('min')
            sorted_data['condition_start_date'] = sorted_data['condition_start_date'].fillna(
                sorted_data['person_id'].map(min_start_date)
            )
            mask = sorted_data['condition_start_date'].isna() & sorted_data['condition_end_date'].notna()
            sorted_data.loc[mask, 'condition_start_date'] = (
                sorted_data.loc[mask, 'condition_end_date'] - pd.Timedelta(days=30)
            )

            median_date = sorted_data['condition_start_date'].median()
            sorted_data['condition_start_date'] = sorted_data['condition_start_date'].fillna(median_date)

            sorted_data['condition_end_date'] = sorted_data['condition_end_date'].fillna(
                sorted_data['condition_start_date'] + pd.Timedelta(days=30)
            )

            sorted_data['prev_date'] = sorted_data.groupby(
                ['person_id', 'condition_concept_id']
            )['condition_end_date'].shift(1)
            sorted_data['new_era'] = (
                sorted_data['prev_date'].isna()
            ) | ((sorted_data['condition_start_date'] - sorted_data['prev_date']).dt.days > window_size)
            sorted_data['era'] = sorted_data.groupby(
                ['person_id', 'condition_concept_id']
            )['new_era'].cumsum()
            sorted_data = sorted_data.groupby(['person_id', 'condition_concept_id', 'era']).agg(
                condition_era_start_date=('condition_start_date', 'first'),
                condition_era_end_date=('condition_end_date', 'last'),
            ).reset_index()

            sorted_data['condition_era_source'] = sorted_data[
                ['person_id', 'condition_concept_id', 'condition_era_start_date', 'condition_era_end_date']
            ].astype(str).agg('_'.join, axis=1)
            sorted_data['condition_era_id'] = sorted_data['condition_era_source'].apply(
                self._query_utils.unique_id_generator, source_type='condition era'
            )

            queried_condition_era = self._query_utils.retrieve_condition_era()
            existing_condition_era = set(queried_condition_era['condition_era_id'])
            filtered_data = sorted_data[
                ~sorted_data['condition_era_id'].isin(existing_condition_era)
            ]
            if filtered_data.empty:
                logging.info("No new data to insert for condition era; all records already exist in the target table.")
                return

            filtered_data.drop(columns=['era', 'condition_era_source'], inplace=True)
            filtered_data = filtered_data.drop_duplicates(subset=['condition_era_id'], keep='first')

            asyncio.run(self._push_to_db(
                batch_size=250000,
                data=filtered_data,
                table_name='condition_era'
            ))
            logging.info(f"Loaded data into table '{self._schema}.condition_era'.")

        except Exception as e:
            logging.error(f"Failed to load data into table: {e}")
