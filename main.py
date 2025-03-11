from mappers.synthea_mapper import SyntheaETLPipeline
from mappers.custom_mapper import CustomETLPipeline
from mappers.main_mapper import BaseETLPipeline
from scripts.csv_gen.main import CSVGen
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
    loader = BaseETLPipeline()
    loader.db_connector._db_loader.load_all_csvs(vocab)
    


if __name__ == "__main__":
    main()
    # generate_csv()
    # generate_ddl()
    # load_vocab()