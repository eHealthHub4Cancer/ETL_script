import pandas as pd

from scripts.etls.person_etl import Person
from scripts.etls.death_etl import Death
from scripts.etls.condition_etl import Condition
from scripts.etls.drug_exposure_etl import DrugExposure
from scripts.etls.encounter_etl import Encounters
from scripts.etls.observation_period_etl import ObservationPeriod
from scripts.etls.obs_measurement_etl import ObserMeasurement
from scripts.etls.observation_etl import Observation


def _run_etl(etl_cls, data):
    etl = etl_cls(file_path="unused.csv", table_name="test", fields_map=list(data.columns))
    etl._source_data = data.copy()
    etl.map_data()
    return etl._source_data


def test_person_maps_birth_and_gender():
    data = pd.DataFrame(
        {
            "id": ["p1"],
            "gender": ["Male"],
            "race": ["White"],
            "ethnicity": ["Hispanic"],
            "birthdate": ["1980-01-02"],
            "zip": ["12345"],
        }
    )
    mapped = _run_etl(Person, data)
    assert mapped["gender_concept_id"].iloc[0] == 8507
    assert mapped["year_of_birth"].iloc[0] == 1980
    assert mapped["birth_datetime"].iloc[0] == pd.Timestamp("1980-01-02")


def test_death_filters_pre_birth():
    data = pd.DataFrame(
        {
            "id": ["p1", "p2"],
            "birthdate": ["1980-01-02", "1990-01-01"],
            "deathdate": ["1979-12-31", "2020-01-01"],
        }
    )
    mapped = _run_etl(Death, data)
    assert len(mapped) == 1
    assert mapped["death_date"].iloc[0] == pd.Timestamp("2020-01-01").date()


def test_condition_normalizes_date_ranges():
    data = pd.DataFrame(
        {
            "patient": ["p1"],
            "start": ["2020-01-10"],
            "stop": ["2020-01-01"],
            "description": ["Condition"],
            "code": ["C1"],
            "encounter": ["e1"],
        }
    )
    mapped = _run_etl(Condition, data)
    assert mapped["condition_start_date"].iloc[0] == mapped["condition_end_date"].iloc[0]


def test_drug_exposure_normalizes_date_ranges():
    data = pd.DataFrame(
        {
            "patient": ["p1"],
            "start": ["2020-01-10"],
            "stop": ["2020-01-01"],
            "description": ["Drug"],
            "code": ["D1"],
            "encounter": ["e1"],
        }
    )
    mapped = _run_etl(DrugExposure, data)
    assert mapped["drug_exposure_start_date"].iloc[0] == mapped["drug_exposure_end_date"].iloc[0]


def test_encounter_normalizes_missing_end():
    data = pd.DataFrame(
        {
            "id": ["v1"],
            "patient": ["p1"],
            "provider": ["pr1"],
            "organization": ["org1"],
            "encounterclass": ["outpatient"],
            "start": ["2020-01-01"],
            "stop": [pd.NaT],
        }
    )
    mapped = _run_etl(Encounters, data)
    assert mapped["visit_end_date"].iloc[0] == mapped["visit_start_date"].iloc[0]


def test_observation_period_uses_valid_ranges():
    data = pd.DataFrame(
        {
            "patient": ["p1", "p1"],
            "start": [pd.NaT, "2020-02-01"],
            "stop": ["2020-01-15", "2020-03-01"],
        }
    )
    mapped = _run_etl(ObservationPeriod, data)
    assert mapped["observation_period_start_date"].iloc[0] <= mapped["observation_period_end_date"].iloc[0]


def test_measurement_and_observation_drop_missing_dates():
    measurement_data = pd.DataFrame(
        {
            "patient": ["p1", "p2"],
            "date": ["2020-01-01", pd.NaT],
            "category": ["laboratory", "laboratory"],
            "description": ["Lab", "Lab"],
            "code": ["L1", "L1"],
            "value": ["5", "7"],
            "units": ["mg", "mg"],
            "encounter": ["e1", "e2"],
        }
    )
    observation_data = pd.DataFrame(
        {
            "patient": ["p1", "p2"],
            "date": ["2020-01-01", pd.NaT],
            "category": ["health indicator", "health indicator"],
            "description": ["Obs", "Obs"],
            "code": ["QOLS", "QOLS"],
            "value": ["1", "2"],
            "units": ["score", "score"],
            "encounter": ["e1", "e2"],
        }
    )
    mapped_measurement = _run_etl(ObserMeasurement, measurement_data)
    mapped_observation = _run_etl(Observation, observation_data)
    assert len(mapped_measurement) == 1
    assert len(mapped_observation) == 1
