from mappers.main_mapper import BaseETLPipeline
from scripts.etls.person_etl import Person
from scripts.etls.observation_period_etl import ObservationPeriod
from scripts.loaders.load_person import LoadPerson
from scripts.loaders.load_obser_period import LoadObservationPeriod
from scripts.etls.encounter_etl import Encounters
from scripts.loaders.load_encounter import LoadEncounter
from scripts.etls.location_etl import Location
from scripts.loaders.load_location import LoadLocation
from scripts.etls.death_etl import Death
from scripts.loaders.load_death import LoadDeath
from scripts.etls.care_site_etl import CareSite
from scripts.loaders.load_care import LoadCareSite
from scripts.etls.provider_etl import Provider
from scripts.loaders.load_provider import LoadProvider
from scripts.etls.condition_etl import Condition
from scripts.loaders.load_condition import LoadCondition
from scripts.etls.visit_detail_etl import VisitDetail
from scripts.loaders.load_visit_detail import LoadVisitDetails
from scripts.etls.procedure_etl import Procedure
from scripts.loaders.load_procedure import LoadProcedure
from scripts.etls.drug_exposure_etl import DrugExposure
from scripts.etls.immunization_etl import Immunization
from scripts.loaders.load_drug import LoadDrug
from scripts.etls.obs_measurement_etl import ObserMeasurement
from scripts.loaders.load_measurement import LoadMeasurement
from scripts.etls.observation_etl import Observation
from scripts.loaders.load_observation import LoadObservation


class SyntheaETLPipeline(BaseETLPipeline):
    def __init__(self):
        super().__init__()
        self.files_to_map = {
            "location_": ["patients.csv"],
            "person_": ["patients.csv"], 
            "death_": ["patients.csv"],
            "observation_period_": ["encounters.csv"],
            "care_site_": ["providers.csv"],
            "provider_": ["providers.csv"],
            "visit_occurrence_": ["encounters.csv"],
            "condition_occurrence_": ["conditions.csv"],
            "visit_detail_": ["encounters.csv"],
            "procedure_occurrence_": ["procedures.csv"],
            "drug_exposure_medication": ["medications.csv"],
            "drug_exposure_immunization": ["immunizations.csv"],
            "measurement_": ["observations.csv"],
            "observation_": ["observations.csv"],
        }
        self.etl_mapping = {
            "location_": (Location, LoadLocation, ['location_id', 'city', 'county', 'country_concept_id', 'country_source_value', 'location_source_value']),
            "person_": (Person, LoadPerson, ['person_id', 'gender_concept_id', 'year_of_birth', 'month_of_birth', 'day_of_birth', 'birth_datetime', 'race_concept_id', 'ethnicity_concept_id', 'person_source_value', 'gender_source_value', 'gender_source_concept_id', 'race_source_value', 'race_source_concept_id', 'ethnicity_source_value', 'ethnicity_source_concept_id', 'location_source_value']),
            "death_": (Death, LoadDeath, ['death_date', 'death_type_concept_id', 'death_datetime', 'cause_concept_id', 'cause_source_value', 'cause_source_concept_id', 'person_source_value']),
            "observation_period_": (ObservationPeriod, LoadObservationPeriod, ['person_source_value', 'observation_period_id', 'observation_period_start_date', 'observation_period_end_date', 'period_type_concept_id']),
            "care_site_": (CareSite, LoadCareSite, ['care_site_id', 'care_site_name', 'place_of_service_concept_id', 'care_site_source_value', 'location_source_value']),
            "provider_": (Provider, LoadProvider, ['provider_id', 'gender_source_value', 'specialty_source_value', 'provider_name', 'provider_source_value', 'specialty_concept_id', 'gender_concept_id', 'care_site_source_value']),
            "visit_occurrence_": (Encounters, LoadEncounter, ['visit_occurrence_id', 'visit_concept_id', 'visit_start_date', 'visit_start_datetime', 'visit_end_date', 'visit_end_datetime', 'person_source_value', 'visit_type_concept_id', 'visit_source_value', 'provider_source_value', 'care_site_source_value']),
            "condition_occurrence_": (Condition, LoadCondition, ['condition_occurrence_id', 'person_source_value', 'condition_start_date', 'condition_start_datetime', 'condition_end_date', 'condition_end_datetime', 'condition_source_value', 'condition_source_concept_id', 'visit_source_value', 'condition_type_concept_id']),
            "visit_detail_": (VisitDetail, LoadVisitDetails, ['visit_detail_id', 'person_source_value', 'visit_detail_concept_id', 'visit_detail_start_date', 'visit_detail_start_datetime', 'visit_detail_end_date', 'visit_detail_end_datetime', 'visit_source_value', 'provider_source_value', 'visit_detail_type_concept_id', 'care_site_source_value',
                                                             'admitted_from_concept_id', 'admitted_from_source_value']),
            "procedure_occurrence_": (Procedure, LoadProcedure, ['procedure_occurrence_id', 'person_source_value', 'procedure_date', 'procedure_datetime', 'procedure_end_date', 'procedure_end_datetime', 'procedure_type_concept_id', 'procedure_source_value', 'procedure_source_concept_id', 'visit_source_value']),
            "drug_exposure_medication": (DrugExposure, LoadDrug, ['drug_exposure_id', 'person_source_value', 'drug_exposure_start_date', 'drug_exposure_start_datetime', 'drug_exposure_end_date', 'drug_exposure_end_datetime', 'drug_source_value', 'drug_source_concept_id', 'visit_source_value', 'drug_type_concept_id']),
            "drug_exposure_immunization": (Immunization, LoadDrug, ['drug_exposure_id', 'person_source_value', 'drug_exposure_start_date', 'drug_exposure_start_datetime', 'drug_exposure_end_date', 'drug_exposure_end_datetime', 'drug_source_value', 'drug_source_concept_id', 'visit_source_value', 'drug_type_concept_id']),
            "measurement_": (ObserMeasurement, LoadMeasurement, ['measurement_id', 'person_source_value', 'measurement_datetime', 'measurement_date', 'measurement_type_concept_id', 'measurement_source_value', 'measurement_concept_id', 'value_as_number', 'value_source_value', 'visit_source_value', 'unit_source_value']),
            "observation_": (Observation, LoadObservation, ['observation_id', 'person_source_value', 'observation_datetime', 'observation_date', 'observation_type_concept_id', 'observation_source_value', 'observation_concept_id', 'visit_source_value', 'value_as_number', 'value_source_value', 'unit_source_value']),
        }

    def run(self):
        super().run(self.etl_mapping, self.files_to_map)
