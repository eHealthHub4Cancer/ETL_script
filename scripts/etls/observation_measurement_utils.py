import pandas as pd

MEASUREMENT_CATEGORY_MAP = {
    'laboratory': 'LP29693-6',
    'vital-signs': 'LP30605-7',
    'exam': 'LP7801-6',
}

OBSERVATION_CATEGORY_MAP = {
    'health indicator': '723111000000104',
}

MEASUREMENT_CATEGORIES = frozenset(MEASUREMENT_CATEGORY_MAP.keys())
OBSERVATION_CATEGORIES = frozenset({
    'health indicator',
    'survey',
    'social-history',
})


def normalize_category(series: pd.Series) -> pd.Series:
    return series.fillna('').astype(str).str.strip().str.lower()


def classify_measurement_rows(source_data: pd.DataFrame) -> pd.Series:
    categories = normalize_category(source_data['category'])
    has_measurement_category = categories.isin(MEASUREMENT_CATEGORIES)
    has_observation_category = categories.isin(OBSERVATION_CATEGORIES)
    value_numeric = pd.to_numeric(source_data['value'], errors='coerce').notna()
    has_units = source_data['units'].fillna('').astype(str).str.strip().ne('')
    return has_measurement_category | (~has_observation_category & value_numeric & has_units)


def map_category(series: pd.Series, category_map: dict) -> pd.Series:
    categories = normalize_category(series)
    return categories.map(category_map).fillna(0)
