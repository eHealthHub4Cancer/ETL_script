from mappers.synthea_mapper import SyntheaETLPipeline
from mappers.custom_mapper import CustomETLPipeline
from mappers.main_mapper import BaseETLPipeline

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

if __name__ == "__main__":
    main()