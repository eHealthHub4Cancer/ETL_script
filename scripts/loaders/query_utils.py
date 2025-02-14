from .main_load import LoadOmoppedData
import logging
from typing import Optional
import pandas as pd
import numpy as np
import rpy2.robjects as ro
from rpy2.robjects.packages import importr
from rpy2.robjects import pandas2ri
import pyarrow.feather as feather

# Configure logging
logging.basicConfig(level=logging.DEBUG)  # Use DEBUG level for detailed logging

class QueryUtils:
    def __init__(self, conn, schema, table, csv_loader):
        """
        Initialize the QueryUtils with the given parameters.

        :param conn: The database connection object.
        :param schema: The schema to be used in the database.
        """
        self._conn = conn
        self._schema = schema
        self._table = table
        self._db_connector = importr('DatabaseConnector')
        self._arrow = importr('arrow')
        self._csv_loader = csv_loader
        
    def convert_dataframe(self, data, direction='r_to_py'):
        """
        Converts a DataFrame between R and pandas.
        source: https://rpy2.github.io/doc/latest/html/generated_rst/pandas.html
        
        Parameters:
        - data: The DataFrame to convert.
        - direction: str, optional, either 'r_to_py' or 'py_to_r'.
                     'r_to_py' for R to pandas conversion (default).
                     'py_to_r' for pandas to R conversion.

        Returns:
        - Converted DataFrame.
        """
        # with (ro.default_converter + pandas2ri.converter).context():
        #     conversion = ro.conversion.get_conversion()
            
        if direction == 'r_to_py':
            # Convert R DataFrame to pandas DataFrame
            logging.debug("Converting R DataFrame to pandas DataFrame.")
            self._arrow.write_feather(data, 'rdf.feather')
            return pd.read_feather('rdf.feather')
        
        elif direction == 'py_to_r':
            # Convert pandas DataFrame to R DataFrame
            logging.debug("Converting pandas DataFrame to R DataFrame.")
            feather.write_feather(data, 'pdf.feather')
            return self._arrow.read_feather('pdf.feather')
        else:
            raise ValueError("Invalid direction. Use 'r_to_py' or 'py_to_r'.")

    def retrieve_persons(self):
        """Retrieve existing person records."""
        query = f"SELECT person_source_value, person_id FROM {self._schema}.person"
        queried_data = self._db_connector.querySql(
            connection=self._conn,
            sql=query
        )
        queried_data_pandas = self.convert_dataframe(queried_data, direction='r_to_py')
        queried_data_pandas.columns = queried_data_pandas.columns.str.lower()
        # convert the data types to appropriate format if not empty
        if not queried_data_pandas.empty:
            queried_data_pandas = self._csv_loader.compare_and_convert(queried_data_pandas, 'person')

        return queried_data_pandas
    
    def retrieve_visit_occurrences(self):
        """Retrieving existing visit_occurrence records."""
        query = f"SELECT visit_occurrence_id FROM {self._schema}.visit_occurrence"
        queried_data = self._db_connector.querySql(
            connection=self._conn,
            sql=query
        )
        queried_data_pandas = self.convert_dataframe(queried_data, direction='r_to_py')
        queried_data_pandas.columns = queried_data_pandas.columns.str.lower()
        # convert if not empty.
        if not queried_data_pandas.empty:
            queried_data_pandas = self._csv_loader.compare_and_convert(queried_data_pandas, 'visit_occurrence')
        return queried_data_pandas
    
    def retrieve_obser_periods(self):
        """Retrieve existing observation period records."""
        query = f"SELECT person_id FROM {self._schema}.observation_period"
        queried_data = self._db_connector.querySql(
            connection=self._conn,
            sql=query
        )
        queried_data_pandas = self.convert_dataframe(queried_data, direction='r_to_py')
        queried_data_pandas.columns = queried_data_pandas.columns.str.lower()
        if not queried_data_pandas.empty:
            queried_data_pandas = self._csv_loader.compare_and_convert(queried_data_pandas, 'observation_period')
        return queried_data_pandas

    
    def retrieve_person_periods(self):
        """Retrieve existing person records and observation records."""

        query = f"""
        SELECT p.person_id, p.person_source_value,
        op.observation_period_start_date, op.observation_period_end_date 
        FROM {self._schema}.person AS p
        JOIN {self._schema}.observation_period AS op
        ON p.person_id = op.person_id
        """
        queried_data = self._db_connector.querySql(
            connection=self._conn,
            sql=query
        )
        queried_data_pandas = self.convert_dataframe(queried_data, direction='r_to_py')
        queried_data_pandas.columns = queried_data_pandas.columns.str.lower()
        return queried_data_pandas
    
    def retrieve_visits(self):
        """Retrieve existing visit records."""
        query = f"SELECT visit_occurrence_id, visit_source_value FROM {self._schema}.visit_occurrence"
        queried_data = self._db_connector.querySql(
            connection=self._conn,
            sql=query
        )
        queried_data_pandas = self.convert_dataframe(queried_data, direction='r_to_py')
        queried_data_pandas.columns = queried_data_pandas.columns.str.lower()
        if not queried_data_pandas.empty:
            queried_data_pandas = self._csv_loader.compare_and_convert(queried_data_pandas, 'visit_occurrence')
        return queried_data_pandas
    
    def retrieve_dated_visits(self):
        """Retrieve existing visit records with dates."""
        query = f"""
        SELECT person_id, MIN(visit_start_date) AS visit_start_date, 
        MAX(visit_end_date) AS visit_end_date 
        FROM {self._schema}.visit_occurrence
        GROUP BY person_id
        """
        queried_data = self._db_connector.querySql(
            connection=self._conn,
            sql=query
        )
        queried_data_pandas = self.convert_dataframe(queried_data, direction='r_to_py')
        queried_data_pandas.columns = queried_data_pandas.columns.str.lower()
        queried_data_pandas['person_id']= self._csv_loader.compare_and_convert(queried_data_pandas[['person_id']], 'visit_occurrence')
        queried_data_pandas['visit_start_date'] = self._csv_loader.compare_and_convert(queried_data_pandas[['visit_start_date']], 'visit_occurrence')
        queried_data_pandas['visit_end_date'] = self._csv_loader.compare_and_convert(queried_data_pandas[['visit_end_date']], 'visit_occurrence')
        return queried_data_pandas
    
    def retrieve_locations(self):
        """Retrieve existing location records."""
        query = f"SELECT location_id, location_source_value FROM {self._schema}.location"
        queried_data = self._db_connector.querySql(
            connection=self._conn,
            sql=query
        )
        queried_data_pandas = self.convert_dataframe(queried_data, direction='r_to_py')
        queried_data_pandas.columns = queried_data_pandas.columns.str.lower()
        if not queried_data_pandas.empty:
            queried_data_pandas = self._csv_loader.compare_and_convert(queried_data_pandas, 'location')
        
        return queried_data_pandas

    def retrieve_death(self):
        """Retrieve existing death records."""
        query = f"SELECT person_id FROM {self._schema}.death"
        queried_data = self._db_connector.querySql(
            connection=self._conn,
            sql=query
        )
        queried_data_pandas = self.convert_dataframe(queried_data, direction='r_to_py')
        queried_data_pandas.columns = queried_data_pandas.columns.str.lower()
        if not queried_data_pandas.empty:
            queried_data_pandas = self._csv_loader.compare_and_convert(queried_data_pandas, 'death')
        return queried_data_pandas
    
    def retrieve_care_sites(self):
        """Retrieve existing care site records."""
        query = f"SELECT care_site_id, care_site_source_value FROM {self._schema}.care_site"
        queried_data = self._db_connector.querySql(
            connection=self._conn,
            sql=query
        )
        queried_data_pandas = self.convert_dataframe(queried_data, direction='r_to_py')
        queried_data_pandas.columns = queried_data_pandas.columns.str.lower()
        if not queried_data_pandas.empty:
            queried_data_pandas = self._csv_loader.compare_and_convert(queried_data_pandas, 'care_site')
        return queried_data_pandas

    def retrieve_providers(self):
        """Retrieve existing provider records."""
        query = f"SELECT provider_id, provider_source_value FROM {self._schema}.provider"
        queried_data = self._db_connector.querySql(
            connection=self._conn,
            sql=query
        )
        queried_data_pandas = self.convert_dataframe(queried_data, direction='r_to_py')
        queried_data_pandas.columns = queried_data_pandas.columns.str.lower()
        
        if not queried_data_pandas.empty:
            queried_data_pandas = self._csv_loader.compare_and_convert(queried_data_pandas, 'provider')
        return queried_data_pandas

    def retrieve_concepts(self):
        """Retrieve existing concept records."""
        query = f"SELECT concept_id, concept_code, vocabulary_id FROM {self._schema}.concept"
        queried_data = self._db_connector.querySql(
            connection=self._conn,
            sql=query
        )
        queried_data_pandas = self.convert_dataframe(queried_data, direction='r_to_py')
        queried_data_pandas.columns = queried_data_pandas.columns.str.lower()
        
        if not queried_data_pandas.empty:
            queried_data_pandas = self._csv_loader.compare_and_convert(queried_data_pandas, 'concept')
        
        return queried_data_pandas
    
    def retrieve_conditions(self):
        """Retrieve existing condition records."""
        query = f"SELECT condition_occurrence_id, condition_source_value FROM {self._schema}.condition_occurrence"
        queried_data = self._db_connector.querySql(
            connection=self._conn,
            sql=query
        )
        queried_data_pandas = self.convert_dataframe(queried_data, direction='r_to_py')
        queried_data_pandas.columns = queried_data_pandas.columns.str.lower()
        
        if not queried_data_pandas.empty:
            queried_data_pandas = self._csv_loader.compare_and_convert(queried_data_pandas, 'condition_occurrence')
        
        return queried_data_pandas
    
    def retrieve_visit_details(self):
        """Retrieve existing visit detail records."""
        query = f"SELECT visit_detail_id, visit_detail_source_value FROM {self._schema}.visit_detail"
        queried_data = self._db_connector.querySql(
            connection=self._conn,
            sql=query
        )
        queried_data_pandas = self.convert_dataframe(queried_data, direction='r_to_py')
        queried_data_pandas.columns = queried_data_pandas.columns.str.lower()
        
        if not queried_data_pandas.empty:
            queried_data_pandas = self._csv_loader.compare_and_convert(queried_data_pandas, 'visit_detail')
        
        return queried_data_pandas

    def retrieve_procedures(self):
        """Retrieve existing procedure records."""
        query = f"SELECT procedure_occurrence_id, procedure_source_value FROM {self._schema}.procedure_occurrence"
        queried_data = self._db_connector.querySql(
            connection=self._conn,
            sql=query
        )
        queried_data_pandas = self.convert_dataframe(queried_data, direction='r_to_py')
        queried_data_pandas.columns = queried_data_pandas.columns.str.lower()
        
        if not queried_data_pandas.empty:
            queried_data_pandas = self._csv_loader.compare_and_convert(queried_data_pandas, 'procedure_occurrence')
        
        return queried_data_pandas
    
    def retrieve_drugs(self):
        """Retrieve existing drug records."""
        query = f"SELECT drug_exposure_id, drug_source_value FROM {self._schema}.drug_exposure"
        queried_data = self._db_connector.querySql(
            connection=self._conn,
            sql=query
        )
        queried_data_pandas = self.convert_dataframe(queried_data, direction='r_to_py')
        queried_data_pandas.columns = queried_data_pandas.columns.str.lower()
        
        if not queried_data_pandas.empty:
            queried_data_pandas = self._csv_loader.compare_and_convert(queried_data_pandas, 'drug_exposure')
        
        return queried_data_pandas
    
    def retrieve_measurements(self):
        """Retrieve existing measurement records."""
        query = f"SELECT measurement_id, measurement_source_value FROM {self._schema}.measurement"
        queried_data = self._db_connector.querySql(
            connection=self._conn,
            sql=query
        )
        queried_data_pandas = self.convert_dataframe(queried_data, direction='r_to_py')
        queried_data_pandas.columns = queried_data_pandas.columns.str.lower()
        
        if not queried_data_pandas.empty:
            queried_data_pandas = self._csv_loader.compare_and_convert(queried_data_pandas, 'measurement')
        
        return queried_data_pandas
    
    def retrieve_observations(self):
        """Retrieve existing observation records."""
        query = f"SELECT observation_id, observation_source_value FROM {self._schema}.observation"
        queried_data = self._db_connector.querySql(
            connection=self._conn,
            sql=query
        )
        queried_data_pandas = self.convert_dataframe(queried_data, direction='r_to_py')
        queried_data_pandas.columns = queried_data_pandas.columns.str.lower()
        
        if not queried_data_pandas.empty:
            queried_data_pandas = self._csv_loader.compare_and_convert(queried_data_pandas, 'observation')
        
        return queried_data_pandas