from mappers.synthea_mapper import SyntheaETLPipeline
from mappers.custom_mapper import CustomETLPipeline
from mappers.main_mapper import BaseETLPipeline
from scripts.loaders.connector import ConnectToDatabase
from scripts.csv_gen.main import CSVGen
from scripts.usagi.main import MapCodeGen
import ast

# import dotenv
from dotenv import load_dotenv
import os
load_dotenv()

def main():
    mapper_class = os.getenv("MAPPER_CLASS")
    if mapper_class:
        etl = SyntheaETLPipeline()
    else:
        etl = CustomETLPipeline()
    
    etl.run()

def generate_csv():
    # tables we want to generate csv files from
    table_names=os.getenv("TABLE_NAMES")
    
    if table_names:
        table_names = ast.literal_eval(table_names)
    else:
        table_names = []    
    # directory to save the csv files
    csv_results = os.getenv("OMOP_CSV_RESULT")
    # set the schema
    schema = os.getenv("DB_SCHEMA")
    # creating connection to the database
    db_conn = BaseETLPipeline()
    # creating the CSVGen object
    csv_gen = CSVGen(db_conn.db_connector._conn, table_names, csv_results, schema)
    csv_gen.generate_csv()

# this part create the ddl scripts.
def generate_ddl():
    # get the cdm version
    cdm_version = os.getenv("CDM_VERSION")
    loader = BaseETLPipeline()
    loader.db_connector._conn_details.execute_ddl(cdm_version)

# load the vocabulary
def load_vocab():
    vocab = os.getenv("CSV_PATH")
    vocab_schema = os.getenv("VOCAB_SCHEMA") or os.getenv("DB_SCHEMA")
    loader = BaseETLPipeline()
    if vocab_schema and vocab_schema != loader.db_connector._schema:
        db_config = loader.db_config.copy()
        db_config["db_schema"] = vocab_schema
        vocab_loader = ConnectToDatabase(**db_config)
        vocab_loader._db_loader.load_all_csvs(vocab)
    else:
        loader.db_connector._db_loader.load_all_csvs(vocab)

# generate the mapping code.
def generate_mapping():
    table_names = os.getenv("NULL_CONCEPT_TABLES")
    if table_names:
        table_names = ast.literal_eval(table_names)
    else:
        table_names = []

    file_name = os.getenv("FILE_NAME")
    
    loader = BaseETLPipeline()
    schema = os.getenv("DB_SCHEMA")
    vocab_schema = os.getenv("VOCAB_SCHEMA") or schema
    usagi_result = os.getenv("USAGI_RESULT") 
    generator = MapCodeGen(
        db_conn=loader.db_connector._conn, 
        table_names=table_names, 
        save_dir=usagi_result, 
        schema=schema,
        vocab_schema=vocab_schema,
        file_name=file_name)
    # generator.run()    
    get_data = generator.save_usagi(file_name="usagi_result.csv")
    generator.push_usagi(
        connector=loader.db_connector, 
        data=get_data,
        table_name="source_to_concept_map",
        batch_size=250000
    )



if __name__ == "__main__":
    # main()
    generate_mapping()
    # generate_csv()
    # generate_ddl()
    # load_vocab()
