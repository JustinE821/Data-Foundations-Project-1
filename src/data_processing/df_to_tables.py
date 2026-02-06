"""
This file is responsible for step 3 in our data processing pipeline. That is, breaking apart our 
combined DataFrame into smaller DataFrames corresponding to each table in our database.
"""
from data_processing.constants import WILDFIRE_COLUMN_NAMES, WILDFIRE_SIZE_COLUMN_NAMES, WILDFIRE_SIZE_CLASS_COLUMN_NAMES
from data_processing.constants import WILDFIRE_CAUSE_COLUMN_NAMES, WILDFIRE_LOCATION_COLUMN_NAMES
