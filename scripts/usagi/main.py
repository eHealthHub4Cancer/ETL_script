import os
import pandas as pd
from rpy2.robjects.packages import importr
import logging
from scripts.loaders.query_utils import QueryUtils
import pyarrow.feather as feather
from tqdm import tqdm
from scripts.usagi.table_mappers import TableMapper
# from scripts.loaders.main_load import LoadOmoppedData
import asyncio

pd.set_option('future.no_silent_downcasting', True)

# Configure logging
logging.basicConfig(level=logging.DEBUG)  # Use DEBUG level for detailed logging

class MapCodeGen:
    def __init__(
        self,
        db_conn,
        table_names: list,
        save_dir: str,
        schema: str,
        vocab_schema: str,
        file_name: str = "mapping.csv",
        chunk_size: int = 100000
    ):
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
        self._chunk_size = chunk_size
        self._file_name = file_name
        self._vocab_schema = vocab_schema or schema
        self._df = pd.DataFrame(columns=["concept_id", "source_value", "source_id", "table_name", "field_type"])

        # Ensure the save directory exists
        os.makedirs(self._save_dir, exist_ok=True)
        self._query_utils = QueryUtils(self._conn, self._schema, "", "", self._vocab_schema)

    async def push_to_db(self, batch_size, data, table_name):
        """Push data to the database."""
        try:
            await self._db_loader.bulk_load_data(
                batch_size=batch_size,
                data=data,
                table_name=table_name
            )
        except Exception as e:
            logging.error(f"Failed to load data into table: {e}")
        return

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
                result = self._query_utils.retrieve_null_concepts(table, concept)
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

    def run(self):
        """
        Run the MapCodeGen process.
        """
        self.generate_map()
        self.convert_to_csv(self._file_name)
                
    def load_usagi(self, dir_path: str = "", file_name: str =""):
        try:
            if dir_path:
                self._save_dir = dir_path
            if file_name:
                self._file_name = file_name
            # read the csv file
            file_path = os.path.join(self._save_dir, self._file_name)
            
            if not os.path.exists(file_path):
                logging.error(f"File not found: {file_path}")
                return
            
            chunks = pd.read_csv(file_path, chunksize=self._chunk_size, low_memory=False)
            # list to hold chunk while loading.
            df_list = []
            for chunk in tqdm(chunks, desc=f"Reading CSV for usagi mapper in chunks..."):
                df_list.append(chunk)
            self._source_data = pd.concat(df_list, ignore_index=True).rename(columns=str.lower)
            logging.info(f"Data loaded successfully\n\n")
        
        except FileNotFoundError:
            logging.error(f"File not found: {file_path}")
        
        except pd.errors.ParserError:
            logging.error(f"Error parsing file: {file_path}")
        
        except Exception as e:
            logging.error(f"Unexpected error loading data: {e}")

    def save_usagi(self, dir_path: str = "", file_name: str = ""):
        
        self.load_usagi(dir_path=dir_path, file_name=file_name)
        new_data = self._query_utils.retrieve_all_stcm("source_to_concept_map")
        # lower the columns
        new_data.columns = new_data.columns.str.lower()
        new_data['source_code_description'] = new_data['source_code_description'].str.lower()
        # get unique values based on source_concept_id and source_code_description
        new_data = set(
            tuple(row) for row in new_data[['source_concept_id', 'source_code_description']].dropna().values
        )        # retain only the rows that are not in the new data
        self._source_data['source_code_description'] = self._source_data['source_code_description'].str.lower()
        unique_rows = self._source_data[
            ~self._source_data[['source_concept_id', 'source_code_description']].apply(tuple, axis=1).isin(new_data)
        ]
        
        # check if it is empty.
        if unique_rows.empty:
            logging.info("No new data to insert for usagi; all records already exist in the target table.")
            return
        
        # get the unique values
        unique_rows = unique_rows.drop_duplicates(subset=['source_concept_id', 'source_code_description'], keep='first')
        return unique_rows
    
    def push_usagi(self, connector, data, table_name, batch_size=250000):
        try:
            if data.empty:
                logging.info("smiles")
                return
            # load_omop = LoadOmoppedData(connector, data, table_name)
            loader = connector._db_loader
            # push the filtered data to the database
            asyncio.run(loader.bulk_load_data(
                batch_size=batch_size,
                data=data,
                table_name=table_name
            ))
            logging.info(f"Loaded data into table '{self._schema}.{table_name}'.")
        
        except Exception as e:
            logging.error(f"Failed to load data into table: {e}")
