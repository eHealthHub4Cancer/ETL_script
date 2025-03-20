# This file gets the corresponding fields for each table based on the concept ids.

class TableMapper:
    def __init__(self):
        self._fields = []

    def call_table(self, table_name, concept_id):
        """
        Retrieves the corresponding fields for a table based on the concept ID.

        Args:
            table_name (str): The name of the table.
            concept_id (str): The concept ID to retrieve.
        """
        # convert the table name to lowercase
        table_name = table_name.lower()
        # check if the method exists
        if not hasattr(self, table_name):
            return None
        
        # call the appropriate function based on the table name
        return getattr(self, table_name)(concept_id)

    def person(self, concept_id):
    
        concept_fields = {
            "gender_concept_id": ['gender_concept_id', 'gender_source_value', 'gender_source_concept_id'],
            "ethnicity_concept_id": ['ethnicity_concept_id', 'ethnicity_source_value', 'ethnicity_source_concept_id'],
            "race_concept_id": ['race_concept_id', 'race_source_value', 'race_source_concept_id']
        }

        # get fields, default to an empty list if not found
        self._fields = concept_fields.get(concept_id.lower(), [])
        return self._fields
    
    def condition_occurrence(self, concept_id):
        concept_fields = {
            "condition_concept_id": ['condition_concept_id', 'condition_source_value', 'condition_source_concept_id'],
            "condition_type_concept_id": ['condition_type_concept_id', 'condition_source_value', 'condition_source_concept_id']
        
        }
        self._fields = concept_fields.get(concept_id.lower(), [])
        return self._fields
    
    def drug_exposure(self, concept_id):
        concept_fields = {
            'drug_concept_id': ['drug_concept_id', 'drug_source_value', 'drug_source_concept_id'],
            'drug_type_concept_id': ['drug_type_concept_id', 'drug_source_value', 'drug_source_concept_id'],
        }
        self._fields = concept_fields.get(concept_id.lower(), [])
        return self._fields
    
    def measurement(self, concept_id):
        concept_fields = {
            "measurement_concept_id": ['measurement_concept_id', 'measurement_source_value', 'measurement_source_concept_id'],
            "measurement_type_concept_id": ['measurement_type_concept_id', 'measurement_source_value', 'measurement_source_concept_id']
        }
        self._fields = concept_fields.get(concept_id.lower(), [])
        return self._fields
    
    def observation(self, concept_id):
        concept_fields = {
            "observation_concept_id": ['observation_concept_id', 'observation_source_value', 'observation_source_concept_id'],
            "observation_type_concept_id": ['observation_type_concept_id', 'observation_source_value', 'observation_source_concept_id']
        }
        self._fields = concept_fields.get(concept_id.lower(), [])
        return self._fields
    
    def procedure_occurrence(self, concept_id):
        concept_fields = {
            "procedure_concept_id": ['procedure_concept_id', 'procedure_source_value', 'procedure_source_concept_id'],
            "procedure_type_concept_id": ['procedure_type_concept_id', 'procedure_source_value', 'procedure_source_concept_id']
        }
        self._fields = concept_fields.get(concept_id.lower(), [])
        return self._fields
    
    def specimen(self, concept_id):
        concept_fields = {
            "specimen_concept_id": ['specimen_concept_id', 'specimen_source_value', 'specimen_source_id'],
            "specimen_type_concept_id": ['specimen_type_concept_id', 'specimen_source_value', 'specimen_source_id']
        }
        self._fields = concept_fields.get(concept_id.lower(), [])
        return self._fields
    
    def cost(self, concept_id):
        concept_fields = {
            "cost_type_concept_id": ['cost_type_concept_id', 'revenue_code_source_value', 'revenue_code_concept_id'],
            "cost_event_id": ['cost_event_id', 'revenue_code_source_value', 'revenue_code_concept_id'],
            "cost_domain_id": ['cost_domain_id', 'revenue_code_source_value', 'revenue_code_concept_id']
        }
        self._fields = concept_fields.get(concept_id.lower(), [])
        return self._fields
    
    def visit_occurrence(self, concept_id):
        concept_fields = {
            "visit_concept_id": ['visit_concept_id', 'visit_source_value', 'visit_source_concept_id'],
            "visit_type_concept_id": ['visit_type_concept_id', 'visit_source_value', 'visit_source_concept_id']
        }
        self._fields = concept_fields.get(concept_id.lower(), [])
        return self._fields
    
    def visit_detail(self, concept_id):
        concept_fields = {
            "visit_detail_concept_id": ['visit_detail_concept_id', 'visit_detail_source_value', 'visit_detail_source_concept_id'],
            "visit_detail_type_concept_id": ['visit_detail_type_concept_id', 'visit_detail_source_value', 'visit_detail_source_concept_id']
        }
        self._fields = concept_fields.get(concept_id.lower(), [])
        return self._fields
    
    def device_exposure(self, concept_id):
        concept_fields = {
            "device_concept_id": ['device_concept_id', 'device_source_value', 'device_source_concept_id'],
            "device_type_concept_id": ['device_type_concept_id', 'device_source_value', 'device_source_concept_id']
        }
        self._fields = concept_fields.get(concept_id.lower(), [])
        return self._fields
    
    def drug_strength(self, concept_id):
        concept_fields = {
            "ingredient_concept_id": ['ingredient_concept_id', 'invalid_reason', 'ingredient_concept_id'],
        }
        self._fields = concept_fields.get(concept_id.lower(), [])
        return self._fields
    
    def death(self, concept_id):
        concept_fields = {
            "death_type_concept_id": ['death_type_concept_id', 'cause_source_value', 'cause_source_concept_id'],
        }
        self._fields = concept_fields.get(concept_id.lower(), [])
        return self._fields

    def note(self, concept_id):
        concept_fields = {
            "note_type_concept_id": ['note_type_concept_id', 'note_title', 'note_type_concept_id'],
            "note_class_concept_id": ['note_class_concept_id', 'note_title', 'note_class_concept_id'],
            "encoding_concept_id": ['encoding_concept_id', 'note_title', 'encoding_concept_id'],
            "language_concept_id": ['language_concept_id', 'note_title', 'language_concept_id']
        }
        self._fields = concept_fields.get(concept_id.lower(), [])
        return self._fields

    