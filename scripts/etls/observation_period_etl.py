import pandas as pd
import logging
import uuid
from typing import Optional
from .main_etl import ETLEntity

class ObservationPeriod(ETLEntity):
    def map_data(self):
        """Map the specific fields for the Observation period table"""
        return super().map_data()
    