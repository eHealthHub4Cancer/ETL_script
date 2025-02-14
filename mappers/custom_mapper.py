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


class CustomETLPipeline(BaseETLPipeline):
    def __init__(self):
        super().__init__()
        self.files_to_map = {"provider": ["custom_providers.csv"], "visit_occurrence": ["custom_encounters.csv"]}
        self.etl_mapping = {"provider": (Provider, LoadProvider, ['provider_id', 'provider_name']), "visit_occurrence": (Encounters, LoadEncounter, ['visit_occurrence_id', 'visit_concept_id'])}

    def run(self):
        super().run(self.etl_mapping, self.files_to_map, custom=True)