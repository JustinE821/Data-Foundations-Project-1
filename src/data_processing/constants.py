"""
This file contains a source of truth for constants used in our data processing pipeline. That includes paths to each 
dataset, lists of relevant columns to load from each dataset, the list of standardized column names, and the lists
of column names for our database tables.
"""
from pathlib import Path

# Paths to datasets
SCRIPT_DIR_PATH = Path(__file__).parent.resolve()
CSV_FILES_DIR_RELATIVE_PATH = SCRIPT_DIR_PATH / '..' / '..' / 'data'
NATIONAL_PATH = CSV_FILES_DIR_RELATIVE_PATH / 'National_Interagency_Fire_Occurrence_Sixth_Edition_1992-2020_(Feature_Layer).csv'
NEW_YORK_PATH = CSV_FILES_DIR_RELATIVE_PATH / 'New_York_State_Forest_Ranger_Wildland_Fire_Reporting_Database__Beginning_2008.csv'
OREGON_PATH = CSV_FILES_DIR_RELATIVE_PATH / 'ODF_Fire_Occurrence_Data_2000-2022.csv'
CALIFORNIA_PATH = CSV_FILES_DIR_RELATIVE_PATH / 'California_Historic_Fire_Perimeters_-6236829869961296710.csv'

# Standard column names that we will convert DataFrames into
STANDARD_COLUMN_NAMES = ['longitude', 'latitude', 'state', 'fire_name', 'report_date', 'containment_date', 'cause', 'acreage', 'size_class']

# Mappings for renaming our columns into a standard format
NATIONAL_TO_STANDARD_COLUMN_MAPPING = {'LONGITUDE': 'longitude', 'LATITUDE': 'latitude', 'STATE': 'state', 'FIRE_NAME': 'fire_name', 'DISCOVERY_DATE': 'report_date', 'CONT_DATE': 'containment_date', 'NWCG_GENERAL_CAUSE': 'cause', 'FIRE_SIZE': 'acreage', 'FIRE_SIZE_CLASS': 'size_class'}
NEW_YORK_TO_STANDARD_COLUMN_MAPPING = {'Longitude': 'longitude', 'Latitude': 'latitude', 'Incident Name': 'fire_name', 'Initial Report Date': 'report_date', 'Fire Out Date': 'containment_date', 'Cause': 'cause', 'Acreage': 'acreage'}
OREGON_TO_STANDARD_COLUMN_MAPPING = {'Long_DD': 'longitude', 'Lat_DD': 'latitude', 'FireName': 'fire_name', 'ReportDateTime': 'report_date', 'Control_DateTime': 'containment_date', 'GeneralCause': 'cause', 'EstTotalAcres': 'acreage', 'Size_class': 'size_class'}
CALIFORNIA_TO_STANDARD_COLUMN_MAPPING = {'State': 'state', 'Fire Name': 'fire_name', 'Alarm Date': 'report_date', 'Containment Date': 'containment_date', 'Cause': 'cause', 'GIS Calculated Acres': 'acreage'}

# Column names computed based on the mappings so that you only have to set the column names once
NATIONAL_COLS = NATIONAL_TO_STANDARD_COLUMN_MAPPING.keys()
NEW_YORK_COLS = NEW_YORK_TO_STANDARD_COLUMN_MAPPING.keys()
OREGON_COLS = OREGON_TO_STANDARD_COLUMN_MAPPING.keys()
CALIFORNIA_COLS = CALIFORNIA_TO_STANDARD_COLUMN_MAPPING.keys()
# If we want to load additional columns later on, append those column names to the above lists

# Dictionary for mapping causes from each dataset to a standard set of causes 
cause_dict = {
    'Lightning': 1, 
    'Debris Burning': 2, 
    'Incendiary': 3,
    'Power line': 4,
    'Equipment': 5,
    'Children': 9,
    'Prescribed Fire': 10,
    'Campfire': 2,
    'Miscellaneous': 10,
    'Power Line': 4,
    'Equipment': 5,
    'Fireworks': 7,
    'Railroad': 6,
    'Structure': 10,
    'Power generation/transmission/distribution': 4,
    'Natural': 1,
    'Debris and open burning': 2,
    'Missing data/not specified/undetermined': 11,
    'Recreation and ceremony': 2,
    'Equipment and vehicle use': 5,
    'Arson/incendiarism': 3,
    'Fireworks': 7,
    'Other causes': 10,
    'Railroad operations and maintenance': 6,
    'Smoking': 8,
    'Misuse of fire by a minor': 9,
    'Firearms and explosives use': 10,
    'Under Invest': 11,
    'Arson': 3,
    'Equipment Use': 5,
    'Recreation': 2,
    'Juveniles': 9,
    '1': 1,
    '2': 5,
    '3': 8,
    '4': 2,
    '5': 2,
    '6': 6,
    '7': 3,
    '8': 9,
    '9': 10,
    '10': 5,
    '11': 4,
    '12': 10,
    '13': 10,
    '14': 11,
    '15': 10,
    '16': 5,
    '17': 1,
    '18': 10,
    '19': 2
}


#Below shows the date cutoffs for the datasets


# Names of columns in our database
WILDFIRE_COLUMN_NAMES = ['wildfire_id', 'state', 'fire_name', 'cause_id', 'containment_date', 'report_date']
WILDFIRE_SIZE_COLUMN_NAMES = ['wildfire_id', 'size_class', 'acreage']
# WILDFIRE_SIZE_CLASS_COLUMN_NAMES = ['size_class', 'min_acreage', 'max_acreage']
WILDFIRE_LOCATION_COLUMN_NAMES = ['wildfire_id', 'longitude', 'latitude']
# WILDFIRE_CAUSE_COLUMN_NAMES = ['cause_id', 'cause_text']




