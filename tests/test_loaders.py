import importlib
import uuid

import pandas as pd

from scripts.loaders.load_person import LoadPerson
from scripts.loaders.load_location import LoadLocation
from scripts.loaders.load_obser_period import LoadObservationPeriod
from scripts.loaders.load_care import LoadCareSite
from scripts.loaders.load_provider import LoadProvider
from scripts.loaders.load_encounter import LoadEncounter
from scripts.loaders.load_visit_detail import LoadVisitDetails
from scripts.loaders.load_procedure import LoadProcedure
from scripts.loaders.load_condition import LoadCondition
from scripts.loaders.load_drug import LoadDrug
from scripts.loaders.load_measurement import LoadMeasurement
from scripts.loaders.load_observation import LoadObservation
from scripts.loaders.load_death import LoadDeath


def _empty(columns):
    return pd.DataFrame(columns=columns)


class FakeCSVLoader:
    def compare_and_convert(self, data, table):
        return data


class FakeConnector:
    def __init__(self):
        self._conn = None
        self._conn_details = {}
        self._schema = "cdm"
        self._vocab_schema = "vocab"
        self._db_loader = FakeCSVLoader()


class FakeQueryUtils:
    responses = {}

    def __init__(self, *_args, **_kwargs):
        self.responses = FakeQueryUtils.responses

    def _get(self, key, default):
        return self.responses.get(key, default)

    def retrieve_persons(self):
        return self._get("retrieve_persons", _empty(["person_source_value", "person_id"]))

    def retrieve_locations(self):
        return self._get("retrieve_locations", _empty(["location_id", "location_source_value"]))

    def retrieve_obser_periods(self):
        return self._get("retrieve_obser_periods", _empty(["person_id"]))

    def retrieve_care_sites(self):
        return self._get("retrieve_care_sites", _empty(["care_site_id", "care_site_source_value"]))

    def retrieve_providers(self):
        return self._get("retrieve_providers", _empty(["provider_id", "provider_source_value"]))

    def retrieve_visit_occurrences(self):
        return self._get("retrieve_visit_occurrences", _empty(["visit_occurrence_id"]))

    def retrieve_visits(self):
        return self._get("retrieve_visits", _empty(["visit_occurrence_id", "visit_source_value"]))

    def retrieve_conditions(self):
        return self._get("retrieve_conditions", _empty(["condition_occurrence_id", "condition_source_value"]))

    def retrieve_visit_details(self):
        return self._get("retrieve_visit_details", _empty(["visit_detail_id", "visit_detail_source_value"]))

    def retrieve_procedures(self):
        return self._get("retrieve_procedures", _empty(["procedure_occurrence_id", "procedure_source_value"]))

    def retrieve_drugs(self):
        return self._get("retrieve_drugs", _empty(["drug_exposure_id", "drug_source_value"]))

    def retrieve_measurements(self):
        return self._get("retrieve_measurements", _empty(["measurement_id", "measurement_source_value"]))

    def retrieve_observations(self):
        return self._get("retrieve_observations", _empty(["observation_id", "observation_source_value"]))

    def retrieve_death(self):
        return self._get("retrieve_death", _empty(["person_id"]))

    def retrieve_drug_exposure(self):
        return self._get("retrieve_drug_exposure", _empty([]))

    def retrieve_condition_occurrence(self):
        return self._get("retrieve_condition_occurrence", _empty([]))

    def retrieve_drug_era(self):
        return self._get("retrieve_drug_era", _empty(["drug_era_id"]))

    def retrieve_dose_era(self):
        return self._get("retrieve_dose_era", _empty(["dose_era_id"]))

    def retrieve_condition_era(self):
        return self._get("retrieve_condition_era", _empty(["condition_era_id"]))

    def retrieve_concept_id(self, code, vocabulary):
        return {value: index + 1 for index, value in enumerate(code)}

    def retrieve_source_concept_id(self, code, vocabulary):
        return {value: index + 100 for index, value in enumerate(code)}

    def strip_length(self, data, length=50):
        if isinstance(data, str):
            return data[:length]
        return data

    def unique_id_generator(self, source_id, source_type):
        namespace = uuid.NAMESPACE_DNS
        namespace = uuid.uuid5(namespace, source_type)
        return uuid.uuid5(namespace, source_id).int % (10**9)


def _run_loader(monkeypatch, loader_cls, omop_data, responses, table_name):
    module = importlib.import_module(loader_cls.__module__)
    monkeypatch.setattr(module, "QueryUtils", FakeQueryUtils)
    FakeQueryUtils.responses = responses
    loader = loader_cls(FakeConnector(), omop_data, table_name)
    pushes = []

    async def fake_push_to_db(batch_size, data, table_name):
        pushes.append((table_name, data.copy()))

    monkeypatch.setattr(loader, "push_to_db", fake_push_to_db)
    loader.load_data()
    return pushes


def test_load_location_inserts(monkeypatch):
    omop = pd.DataFrame(
        {
            "location_id": [1],
            "city": ["City"],
            "county": ["County"],
            "country_concept_id": [1],
            "country_source_value": ["US"],
            "location_source_value": ["loc1"],
        }
    )
    pushes = _run_loader(
        monkeypatch,
        LoadLocation,
        omop,
        {"retrieve_locations": _empty(["location_id", "location_source_value"])},
        "location",
    )
    assert pushes and pushes[0][0] == "location"


def test_load_person_inserts(monkeypatch):
    omop = pd.DataFrame(
        {
            "person_id": [1],
            "gender_concept_id": [8507],
            "year_of_birth": [1980],
            "month_of_birth": [1],
            "day_of_birth": [2],
            "birth_datetime": [pd.Timestamp("1980-01-02")],
            "race_concept_id": [8527],
            "ethnicity_concept_id": [38003563],
            "person_source_value": ["p1"],
            "gender_source_value": ["male"],
            "gender_source_concept_id": [0],
            "race_source_value": ["white"],
            "race_source_concept_id": [0],
            "ethnicity_source_value": ["hispanic"],
            "ethnicity_source_concept_id": [0],
            "location_source_value": ["loc1"],
        }
    )
    responses = {
        "retrieve_persons": _empty(["person_source_value", "person_id"]),
        "retrieve_locations": pd.DataFrame(
            {"location_id": [1], "location_source_value": ["loc1"]}
        ),
    }
    pushes = _run_loader(monkeypatch, LoadPerson, omop, responses, "person")
    assert pushes and pushes[0][0] == "person"


def test_load_observation_period_inserts(monkeypatch):
    omop = pd.DataFrame(
        {
            "person_source_value": ["p1"],
            "observation_period_id": [1],
            "observation_period_start_date": [pd.Timestamp("2020-01-01").date()],
            "observation_period_end_date": [pd.Timestamp("2020-12-31").date()],
            "period_type_concept_id": [32827],
        }
    )
    responses = {
        "retrieve_persons": pd.DataFrame(
            {"person_source_value": ["p1"], "person_id": [1]}
        ),
        "retrieve_obser_periods": _empty(["person_id"]),
    }
    pushes = _run_loader(monkeypatch, LoadObservationPeriod, omop, responses, "observation_period")
    assert pushes and pushes[0][0] == "observation_period"


def test_load_care_site_inserts(monkeypatch):
    omop = pd.DataFrame(
        {
            "care_site_id": [1],
            "care_site_name": ["Care"],
            "place_of_service_concept_id": [38004446],
            "care_site_source_value": ["cs1"],
            "location_source_value": ["loc1"],
        }
    )
    responses = {
        "retrieve_locations": pd.DataFrame(
            {"location_id": [1], "location_source_value": ["loc1"]}
        ),
        "retrieve_care_sites": _empty(["care_site_id", "care_site_source_value"]),
    }
    pushes = _run_loader(monkeypatch, LoadCareSite, omop, responses, "care_site")
    assert pushes and pushes[0][0] == "care_site"


def test_load_provider_inserts(monkeypatch):
    omop = pd.DataFrame(
        {
            "provider_id": [1],
            "gender_source_value": ["M"],
            "specialty_source_value": ["Spec"],
            "provider_name": ["Dr"],
            "provider_source_value": ["pr1"],
            "specialty_concept_id": [4216158],
            "gender_concept_id": [8507],
            "care_site_source_value": ["cs1"],
        }
    )
    responses = {
        "retrieve_providers": _empty(["provider_id", "provider_source_value"]),
        "retrieve_care_sites": pd.DataFrame(
            {"care_site_id": [1], "care_site_source_value": ["cs1"]}
        ),
    }
    pushes = _run_loader(monkeypatch, LoadProvider, omop, responses, "provider")
    assert pushes and pushes[0][0] == "provider"


def test_load_encounter_inserts(monkeypatch):
    omop = pd.DataFrame(
        {
            "visit_occurrence_id": [1],
            "visit_concept_id": [9202],
            "visit_start_date": [pd.Timestamp("2020-01-01").date()],
            "visit_start_datetime": [pd.Timestamp("2020-01-01")],
            "visit_end_date": [pd.Timestamp("2020-01-02").date()],
            "visit_end_datetime": [pd.Timestamp("2020-01-02")],
            "person_source_value": ["p1"],
            "visit_type_concept_id": [32827],
            "visit_source_value": ["v1"],
            "provider_source_value": ["pr1"],
            "care_site_source_value": ["cs1"],
        }
    )
    responses = {
        "retrieve_persons": pd.DataFrame(
            {"person_source_value": ["p1"], "person_id": [1]}
        ),
        "retrieve_visit_occurrences": _empty(["visit_occurrence_id"]),
        "retrieve_providers": pd.DataFrame(
            {"provider_id": [1], "provider_source_value": ["pr1"]}
        ),
        "retrieve_care_sites": pd.DataFrame(
            {"care_site_id": [1], "care_site_source_value": ["cs1"]}
        ),
    }
    pushes = _run_loader(monkeypatch, LoadEncounter, omop, responses, "visit_occurrence")
    assert pushes and pushes[0][0] == "visit_occurrence"


def test_load_visit_detail_inserts(monkeypatch):
    omop = pd.DataFrame(
        {
            "visit_detail_id": [1],
            "person_source_value": ["p1"],
            "visit_detail_concept_id": ["111"],
            "visit_detail_start_date": [pd.Timestamp("2020-01-01").date()],
            "visit_detail_start_datetime": [pd.Timestamp("2020-01-01")],
            "visit_detail_end_date": [pd.Timestamp("2020-01-01").date()],
            "visit_detail_end_datetime": [pd.Timestamp("2020-01-01")],
            "visit_source_value": ["v1"],
            "provider_source_value": ["pr1"],
            "visit_detail_type_concept_id": [32817],
            "care_site_source_value": ["cs1"],
            "admitted_from_concept_id": ["222"],
            "admitted_from_source_value": ["source"],
        }
    )
    responses = {
        "retrieve_persons": pd.DataFrame(
            {"person_source_value": ["p1"], "person_id": [1]}
        ),
        "retrieve_visit_details": _empty(["visit_detail_id", "visit_detail_source_value"]),
        "retrieve_visits": pd.DataFrame(
            {"visit_occurrence_id": [1], "visit_source_value": ["v1"]}
        ),
        "retrieve_care_sites": pd.DataFrame(
            {"care_site_id": [1], "care_site_source_value": ["cs1"]}
        ),
        "retrieve_providers": pd.DataFrame(
            {"provider_id": [1], "provider_source_value": ["pr1"]}
        ),
    }
    pushes = _run_loader(monkeypatch, LoadVisitDetails, omop, responses, "visit_detail")
    assert pushes and pushes[0][0] == "visit_detail"


def test_load_procedure_inserts(monkeypatch):
    omop = pd.DataFrame(
        {
            "procedure_occurrence_id": [1],
            "person_source_value": ["p1"],
            "procedure_date": [pd.Timestamp("2020-01-01").date()],
            "procedure_datetime": [pd.Timestamp("2020-01-01")],
            "procedure_end_date": [pd.Timestamp("2020-01-01").date()],
            "procedure_end_datetime": [pd.Timestamp("2020-01-01")],
            "procedure_type_concept_id": [32817],
            "procedure_source_value": ["Proc"],
            "procedure_source_concept_id": ["333"],
            "visit_source_value": ["v1"],
        }
    )
    responses = {
        "retrieve_persons": pd.DataFrame(
            {"person_source_value": ["p1"], "person_id": [1]}
        ),
        "retrieve_procedures": _empty(["procedure_occurrence_id", "procedure_source_value"]),
        "retrieve_visits": pd.DataFrame(
            {"visit_occurrence_id": [1], "visit_source_value": ["v1"]}
        ),
    }
    pushes = _run_loader(monkeypatch, LoadProcedure, omop, responses, "procedure_occurrence")
    assert pushes and pushes[0][0] == "procedure_occurrence"


def test_load_condition_inserts(monkeypatch):
    omop = pd.DataFrame(
        {
            "condition_occurrence_id": [1],
            "person_source_value": ["p1"],
            "condition_start_date": [pd.Timestamp("2020-01-01").date()],
            "condition_start_datetime": [pd.Timestamp("2020-01-01")],
            "condition_end_date": [pd.Timestamp("2020-01-02").date()],
            "condition_end_datetime": [pd.Timestamp("2020-01-02")],
            "condition_source_value": ["Cond"],
            "condition_source_concept_id": ["444"],
            "visit_source_value": ["v1"],
            "condition_type_concept_id": [32817],
        }
    )
    responses = {
        "retrieve_persons": pd.DataFrame(
            {"person_source_value": ["p1"], "person_id": [1]}
        ),
        "retrieve_conditions": _empty(["condition_occurrence_id", "condition_source_value"]),
        "retrieve_visits": pd.DataFrame(
            {"visit_occurrence_id": [1], "visit_source_value": ["v1"]}
        ),
    }
    module = importlib.import_module(LoadCondition.__module__)
    monkeypatch.setattr(module, "QueryUtils", FakeQueryUtils)
    monkeypatch.setattr(LoadCondition, "load_condition_era", lambda *_args, **_kwargs: None)
    FakeQueryUtils.responses = responses
    loader = LoadCondition(FakeConnector(), omop, "condition_occurrence")
    pushes = []

    async def fake_push_to_db(batch_size, data, table_name):
        pushes.append((table_name, data.copy()))

    monkeypatch.setattr(loader, "push_to_db", fake_push_to_db)
    loader.load_data()
    assert pushes and pushes[0][0] == "condition_occurrence"


def test_load_drug_inserts(monkeypatch):
    omop = pd.DataFrame(
        {
            "drug_exposure_id": [1],
            "person_source_value": ["p1"],
            "drug_exposure_start_date": [pd.Timestamp("2020-01-01").date()],
            "drug_exposure_start_datetime": [pd.Timestamp("2020-01-01")],
            "drug_exposure_end_date": [pd.Timestamp("2020-01-02").date()],
            "drug_exposure_end_datetime": [pd.Timestamp("2020-01-02")],
            "drug_source_value": ["Drug"],
            "drug_source_concept_id": ["555"],
            "visit_source_value": ["v1"],
            "drug_type_concept_id": [32817],
        }
    )
    responses = {
        "retrieve_persons": pd.DataFrame(
            {"person_source_value": ["p1"], "person_id": [1]}
        ),
        "retrieve_drugs": _empty(["drug_exposure_id", "drug_source_value"]),
        "retrieve_visits": pd.DataFrame(
            {"visit_occurrence_id": [1], "visit_source_value": ["v1"]}
        ),
    }
    module = importlib.import_module(LoadDrug.__module__)
    monkeypatch.setattr(module, "QueryUtils", FakeQueryUtils)
    monkeypatch.setattr(LoadDrug, "load_drug_era_data", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(LoadDrug, "load_dose_era_data", lambda *_args, **_kwargs: None)
    FakeQueryUtils.responses = responses
    loader = LoadDrug(FakeConnector(), omop, "drug_exposure")
    pushes = []

    async def fake_push_to_db(batch_size, data, table_name):
        pushes.append((table_name, data.copy()))

    monkeypatch.setattr(loader, "push_to_db", fake_push_to_db)
    loader.load_data()
    assert pushes and pushes[0][0] == "drug_exposure"


def test_load_measurement_inserts(monkeypatch):
    omop = pd.DataFrame(
        {
            "measurement_id": [1],
            "person_source_value": ["p1"],
            "measurement_datetime": [pd.Timestamp("2020-01-01")],
            "measurement_date": [pd.Timestamp("2020-01-01").date()],
            "measurement_type_concept_id": ["111"],
            "measurement_source_value": ["Lab"],
            "measurement_concept_id": ["666"],
            "value_as_number": [5.0],
            "value_source_value": ["5"],
            "visit_source_value": ["v1"],
            "unit_source_value": ["mg"],
        }
    )
    responses = {
        "retrieve_persons": pd.DataFrame(
            {"person_source_value": ["p1"], "person_id": [1]}
        ),
        "retrieve_measurements": _empty(["measurement_id", "measurement_source_value"]),
        "retrieve_visits": pd.DataFrame(
            {"visit_occurrence_id": [1], "visit_source_value": ["v1"]}
        ),
    }
    pushes = _run_loader(monkeypatch, LoadMeasurement, omop, responses, "measurement")
    assert pushes and pushes[0][0] == "measurement"


def test_load_observation_inserts(monkeypatch):
    omop = pd.DataFrame(
        {
            "observation_id": [1],
            "person_source_value": ["p1"],
            "observation_datetime": [pd.Timestamp("2020-01-01")],
            "observation_date": [pd.Timestamp("2020-01-01").date()],
            "observation_type_concept_id": ["111"],
            "observation_source_value": ["Obs"],
            "observation_concept_id": ["777"],
            "visit_source_value": ["v1"],
            "value_as_number": [1.0],
            "value_source_value": ["1"],
            "unit_source_value": ["score"],
        }
    )
    responses = {
        "retrieve_persons": pd.DataFrame(
            {"person_source_value": ["p1"], "person_id": [1]}
        ),
        "retrieve_observations": _empty(["observation_id", "observation_source_value"]),
        "retrieve_visits": pd.DataFrame(
            {"visit_occurrence_id": [1], "visit_source_value": ["v1"]}
        ),
    }
    pushes = _run_loader(monkeypatch, LoadObservation, omop, responses, "observation")
    assert pushes and pushes[0][0] == "observation"


def test_load_death_inserts(monkeypatch):
    omop = pd.DataFrame(
        {
            "death_date": [pd.Timestamp("2020-01-01").date()],
            "death_type_concept_id": [32817],
            "death_datetime": [pd.Timestamp("2020-01-01")],
            "cause_concept_id": [0],
            "cause_source_value": ["Unknown"],
            "cause_source_concept_id": [0],
            "person_source_value": ["p1"],
        }
    )
    responses = {
        "retrieve_persons": pd.DataFrame(
            {"person_source_value": ["p1"], "person_id": [1]}
        ),
        "retrieve_death": _empty(["person_id"]),
    }
    pushes = _run_loader(monkeypatch, LoadDeath, omop, responses, "death")
    assert pushes and pushes[0][0] == "death"


def test_condition_era_groups(monkeypatch):
    condition_data = pd.DataFrame(
        {
            "person_id": [1, 1, 1],
            "condition_concept_id": [100, 100, 100],
            "condition_start_date": [
                pd.Timestamp("2020-01-01"),
                pd.Timestamp("2020-01-15"),
                pd.Timestamp("2020-03-01"),
            ],
            "condition_end_date": [
                pd.Timestamp("2020-01-10"),
                pd.Timestamp("2020-01-20"),
                pd.Timestamp("2020-03-10"),
            ],
        }
    )
    responses = {
        "retrieve_condition_occurrence": condition_data,
        "retrieve_condition_era": _empty(["condition_era_id"]),
    }
    module = importlib.import_module(LoadCondition.__module__)
    monkeypatch.setattr(module, "QueryUtils", FakeQueryUtils)
    FakeQueryUtils.responses = responses
    loader = LoadCondition(FakeConnector(), pd.DataFrame(), "condition_occurrence")
    pushes = []

    async def fake_push_to_db(batch_size, data, table_name):
        pushes.append((table_name, data.copy()))

    monkeypatch.setattr(loader, "push_to_db", fake_push_to_db)
    loader.load_condition_era(window_size=30)
    assert pushes and pushes[0][0] == "condition_era"
    assert len(pushes[0][1]) == 2


def test_drug_era_groups(monkeypatch):
    drug_data = pd.DataFrame(
        {
            "person_id": [1, 1, 1],
            "drug_concept_id": [200, 200, 200],
            "drug_exposure_start_date": [
                pd.Timestamp("2020-01-01"),
                pd.Timestamp("2020-01-15"),
                pd.Timestamp("2020-03-01"),
            ],
            "drug_exposure_end_date": [
                pd.Timestamp("2020-01-10"),
                pd.Timestamp("2020-01-20"),
                pd.Timestamp("2020-03-10"),
            ],
        }
    )
    responses = {
        "retrieve_drug_exposure": drug_data,
        "retrieve_drug_era": _empty(["drug_era_id"]),
    }
    module = importlib.import_module(LoadDrug.__module__)
    monkeypatch.setattr(module, "QueryUtils", FakeQueryUtils)
    FakeQueryUtils.responses = responses
    loader = LoadDrug(FakeConnector(), pd.DataFrame(), "drug_exposure")
    pushes = []

    async def fake_push_to_db(batch_size, data, table_name):
        pushes.append((table_name, data.copy()))

    monkeypatch.setattr(loader, "push_to_db", fake_push_to_db)
    loader.load_drug_era_data(window_size=30)
    assert pushes and pushes[0][0] == "drug_era"
    assert len(pushes[0][1]) == 2


def test_dose_era_groups(monkeypatch):
    drug_data = pd.DataFrame(
        {
            "person_id": [1, 1, 1],
            "drug_concept_id": [200, 200, 200],
            "drug_exposure_start_date": [
                pd.Timestamp("2020-01-01"),
                pd.Timestamp("2020-01-10"),
                pd.Timestamp("2020-01-15"),
            ],
            "drug_exposure_end_date": [
                pd.Timestamp("2020-01-05"),
                pd.Timestamp("2020-01-12"),
                pd.Timestamp("2020-01-20"),
            ],
            "quantity": [5, 5, 10],
            "dose_unit_concept_id": [1, 1, 1],
        }
    )
    responses = {
        "retrieve_drug_exposure": drug_data,
        "retrieve_dose_era": _empty(["dose_era_id"]),
    }
    module = importlib.import_module(LoadDrug.__module__)
    monkeypatch.setattr(module, "QueryUtils", FakeQueryUtils)
    FakeQueryUtils.responses = responses
    loader = LoadDrug(FakeConnector(), pd.DataFrame(), "drug_exposure")
    pushes = []

    async def fake_push_to_db(batch_size, data, table_name):
        pushes.append((table_name, data.copy()))

    monkeypatch.setattr(loader, "push_to_db", fake_push_to_db)
    loader.load_dose_era_data(window_size=30)
    assert pushes and pushes[0][0] == "dose_era"
    assert len(pushes[0][1]) == 2
