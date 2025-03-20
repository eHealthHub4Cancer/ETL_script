import os
import pandas as pd
from rpy2.robjects.packages import importr
import logging
from scripts.loaders.query_utils import QueryUtils
import pyarrow.feather as feather
from scripts.usagi.table_mappers import TableMapper

pd.set_option('future.no_silent_downcasting', True)

# Configure logging
logging.basicConfig(level=logging.DEBUG)  # Use DEBUG level for detailed logging

class MapCodeGen:
    def __init__(self, db_conn, table_names: list, save_dir: str, schema: str):
        """
        Initialize the MapCodeGen class.

        :param db_conn: Database connection object
        :param table_names: List of table names to export
        :param save_dir: Directory where CSV files will be saved
        """
        self._conn = db_conn
        self._db_connector = importr('DatabaseConnector')
        self._arrow = importr('arrow')
        self._table_names = table_names
        self._save_dir = save_dir
        self._schema = schema
        self._df = pd.DataFrame(columns=["concept_id", "source_value", "source_id", "table_name", "field_type"])

        # Ensure the save directory exists
        os.makedirs(self._save_dir, exist_ok=True)

    def arrange_map(self):
        """
        Arrange the fields we are interested in for each omop tables
        """
        # define concept ids
        CONCEPT_IDS = {}
        # clinical tables
        CLINICAL_CONCEPT_IDS = {
            "condition_occurrence": ["condition_concept_id", "condition_type_concept_id"],
            "drug_exposure": ["drug_concept_id", "drug_type_concept_id"],
            "measurement": ["measurement_concept_id", "measurement_type_concept_id"],
            "observation": ["observation_concept_id", "observation_type_concept_id"],
            "procedure_occurrence": ["procedure_concept_id", "procedure_type_concept_id"],
            "note": ["note_type_concept_id", "note_class_concept_id", "encoding_concept_id", "language_concept_id"],
            "specimen": ["specimen_concept_id"],
        }
        # demographic tables
        DEMOGRAPHIC_CONCEPT_IDS = {
            "person": ["gender_concept_id", "race_concept_id", "ethnicity_concept_id"],
        }
        # cost and health economics tables
        COST_HEALTH_CONCEPT_IDS = {
            "cost": ["cost_type_concept_id", "cost_event_id", "cost_domain_id"],
        }
        # health system data tables
        HEALTH_SYSTEM_CONCEPT_IDS = {
            "visit_occurrence": ["visit_concept_id", "visit_type_concept_id"],
            "visit_detail": ["visit_detail_concept_id", "visit_detail_type_concept_id"],
            "device_exposure": ["device_concept_id", "device_type_concept_id"],
            "drug_strength": ["ingredient_concept_id"],
            "death": ["death_type_concept_id"]
        }

        # grouping the concept ids
        CONCEPT_IDS.update(CLINICAL_CONCEPT_IDS)
        CONCEPT_IDS.update(DEMOGRAPHIC_CONCEPT_IDS)
        CONCEPT_IDS.update(COST_HEALTH_CONCEPT_IDS)
        CONCEPT_IDS.update(HEALTH_SYSTEM_CONCEPT_IDS)

        return CONCEPT_IDS

    def generate_map(self):
        """
        Generates a mapping of concept IDs across different tables in the schema.

        This function retrieves concept IDs from different tables, processes missing values, 
        calculates frequency, and saves the result as a CSV.

        Returns:
            pd.DataFrame: The final processed DataFrame.
        """

        # Initialize QueryUtils class for handling queries
        query_utils = QueryUtils(self._conn, self._schema, "", "")
        # table mapper object
        table_mapper = TableMapper()
        # Retrieve the mapping of concept IDs for different tables
        concept_id_map = self.arrange_map()

        # Create an empty DataFrame with predefined column names

        # Iterate through the tables
        for table in self._table_names:
            table_lower = table.lower()

            # Get concept IDs associated with the table (default to an empty list)
            concept_ids = concept_id_map.get(table_lower, [])
            if not concept_ids:
                logging.info(f"⚠️ No concept IDs found for {table}, skipping...")
                continue

            for concept in concept_ids:
                logging.info(f"🔍 Processing {concept} for {table}")
                concept = concept.lower()
                # Retrieve rows where concept values are null
                result = query_utils.retrieve_null_concepts(table, concept)
                # Skip empty results
                if result.empty:
                    logging.info(f"⚠️ Empty result for {concept} in {table}, skipping...")
                    continue
                # Extract fields from the table mapper.
                fields = table_mapper.call_table(table, concept)
                if not fields:
                    logging.info(f"⚠️ No fields found for {concept} in {table}, skipping...")
                    continue

                if fields is None:
                    logging.info(f"⚠️ Table '{table}' not found in the table mapper, skipping...")
                    continue
                
                # unpack the fields
                concept_id_col, source_value_col, source_id_col = fields                           
                # Ensure these columns exist in the result DataFrame
                for col, default_value in [(concept_id_col, 0), (source_value_col, ""), (source_id_col, 0)]:
                    if col not in result.columns:
                        result[col] = default_value

                # Create a DataFrame for the current iteration
                temp_df = pd.DataFrame({
                    "concept_id": result[concept_id_col],
                    "source_value": result[source_value_col],
                    "source_id": result[source_id_col],
                    "table_name": table,
                    "field_type": concept_id_col
                })

                # Concatenate results to the main DataFrame
                if not temp_df.empty:
                    self._df = pd.concat([self._df, temp_df], ignore_index=True)

        # Convert data types once after the loop for efficiency
        self._df["concept_id"] = self._df["concept_id"].fillna(0).astype(int)
        self._df["source_id"] = self._df["source_id"].fillna(0).astype(int)

        # Count occurrences of (source_value, source_id) and assign as frequency
        self._df["frequency"] = self._df.groupby(["source_value", "source_id"])["concept_id"].transform("size")

        # Drop duplicates while keeping the first occurrence
        self._df.drop_duplicates(subset=["source_value", "source_id"], keep="first", inplace=True)

        logging.info("✅ Mapping code generated successfully")
    
    def convert_to_csv(self, file_name: str):
        if not file_name.endswith(".csv"):
            logging.error("File name must end with .csv")
            return
        # sort the dataframe based on the frequency
        self._df.sort_values(by="frequency", ascending=False, inplace=True)
        file_path = os.path.join(self._save_dir, file_name)
        self._df.to_csv(file_path, index=False)
        logging.info(f"✅ Results successfully saved to {file_path}")

    def run(self, file_name: str = "mapping.csv"):
        """
        Run the MapCodeGen process.
        """
        self.generate_map()
        self.convert_to_csv(file_name)
                

                


                
                            





