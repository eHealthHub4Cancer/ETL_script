from scripts.person_etl import Person
from scripts.load_person import LoadPerson
import time

import os

def main():
    # load file directory.
    file_dir = "C:/Users/23434813/Desktop/synthea_dataset/csv"

    files_to_map = {"person": "patients.csv", "observation_period": "conditions.csv"}

    for file, file_name in files_to_map.items():
        file_path = os.path.join(file_dir, file_name)
        if file == "person":
            fields = ['person_id', 'gender_concept_id', 'year_of_birth', 'month_of_birth', 'day_of_birth', 'race_concept_id',
                    'ethnicity_concept_id', 'person_source_value', 'gender_source_value', 'gender_source_concept_id',
                    'race_source_value', 'ethnicity_source_value']
            person_data = Person(file_path=file_path, fields_map=fields)
            person_data.run_mapping(fields=fields)
            load_result = LoadPerson("postgresql", "localhost", "postgres", "postgres", "ohdsi_tutorial", "C:/Users/23434813/Desktop/AML data/ohdsi/", 
                                        "ohdsi", person_data.get_omopped_data(), file, 5442)
            
            load_result.load_data()
            time.sleep(2)
            print("done\n\n")

        if file == "observation_period":
            fields = ['person_id', 'observation_period_id', 'observation_period_start_date', 'observation_period_end_date']
            
if __name__ == "__main__":
    main()
