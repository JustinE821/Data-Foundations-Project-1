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

# Names of relevant columns in raw datasets
NATIONAL_COLS = ['LONGITUDE', 'LATITUDE', 'STATE', 'FIRE_NAME', 'DISCOVERY_DATE', 'CONT_DATE', 'NWCG_GENERAL_CAUSE', 'FIRE_SIZE', 'FIRE_SIZE_CLASS']
# Note: New York dataset does not have columns for state or size_class
# We will have to insert a state column with 'NY' for its values
# We will have to calculate the size class for each record based on its acreage 
NEW_YORK_COLS = ['Longitude', 'Latitude', 'Incident Name', 'Initial Report Date', 'Fire Out Date', 'Cause', 'Acreage'] 
# Note: Oregon dataset does not have a state column
# We will have to insert a state column with 'OR' for its values
OREGON_COLS = ['Long_DD', 'Lat_DD', 'FireName', 'ReportDateTime', 'Control_DateTime', 'GeneralCause', 'EstTotalAcres', 'Size_class']
# Note: California dataset does not have longitude, latitude, and size_class columns
# We will have to insert longitude and latitude columns with null values
# We will have to calculate the size class for each record based on its acreage
CALIFONIA_COLS = ['State', 'Fire Name', 'Alarm Date', 'Containment Date', 'Cause', 'GIS Calculated Acres']

# Standard column names that we will convert DataFrames into
STANDARD_COLUMN_NAMES = ['longitude', 'latitude', 'state', 'fire_name', 'report_date', 'containment_date', 'cause', 'acreage', 'size_class']

# Names of columns in our database
WILDFIRE_COLUMN_NAMES = ['wildfire_id', 'state', 'fire_name', 'cause_id', 'containment_date', 'report_date']
WILDFIRE_SIZE_COLUMN_NAMES = ['wildfire_id', 'size_class', 'acreage']
WILDFIRE_SIZE_CLASS_COLUMN_NAMES = ['size_class', 'min_acreage', 'max_acreage']
WILDFIRE_LOCATION_COLUMN_NAMES = ['wildfire_id', 'longitude', 'latitude']
WILDFIRE_CAUSE_COLUMN_NAMES = ['cause_id', 'cause_text']
